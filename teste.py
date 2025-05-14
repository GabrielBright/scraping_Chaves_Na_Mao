import pandas as pd
import os
import asyncio
import sys
import logging
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from time import time

# Configurações
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Arquivos
ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_carros.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

# Constantes
TIMEOUT = 30_000  # 30 segundos
RETRIES = 3
MAX_CONCURRENT = 10  # Ajustado para estabilidade

async def carregar_links():
    """Carrega links únicos do arquivo Excel."""
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS)
        if "Link" not in df.columns:
            logging.error(f"Coluna 'Link' não encontrada em {ARQUIVO_EXCEL_LINKS}.")
            return []
        links = df['Link'].dropna().unique().tolist()
        logging.info(f"Carregados {len(links)} links únicos.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar {ARQUIVO_EXCEL_LINKS}: {e}")
        return []

async def extrair_elemento(pagina, seletor, default="N/A"):
    """Extrai texto de um elemento com seletor, retorna default se falhar."""
    try:
        elemento = pagina.locator(seletor)
        if await elemento.count() > 0:
            texto = (await elemento.first.inner_text(timeout=TIMEOUT)).strip()
            return texto or default
        return default
    except Exception as e:
        logging.debug(f"Erro ao extrair '{seletor}': {e}")
        return default

async def extrair_com_multiplos_seletores(pagina, seletores, default="N/A"):
    """Tenta extrair com múltiplos seletores, retorna o primeiro sucesso."""
    for seletor in seletores:
        valor = await extrair_elemento(pagina, seletor, default)
        if valor != default:
            return valor
    return default

async def extracao_dados(contexto, link, semaphore):
    """Extrai dados de uma página com retries e scroll para carregar conteúdo."""
    async with semaphore:
        pagina = None
        try:
            pagina = await contexto.new_page()
            logging.info(f"Acessando {link}")
            for attempt in range(RETRIES):
                try:
                    response = await pagina.goto(link, timeout=TIMEOUT, wait_until='domcontentloaded')
                    if response and response.status != 200:
                        logging.warning(f"Status {response.status} em {link}.")
                        return None

                    # Scroll para carregar tabela FIPE
                    for _ in range(5):
                        if await pagina.locator("#version-price-fipe").count() > 0:
                            break
                        await pagina.evaluate("window.scrollBy(0, 800)")
                        await asyncio.sleep(0.5)

                    seletores = {
                        "Modelo": '.style-module__icNBzq__mainSection .column.spacing-2x div span p b',
                        "Versão": '.style-module__icNBzq__mainSection .column.spacing-2x div span p small',
                        "Preço": '.style-module__icNBzq__mainSection .column.spacing-2x div div span p b',
                        "Cor": [
                            '//li[contains(text(), "Cor")]/p/b',
                            '//article/article/section[2]/div/div[1]/ul/li[7]/p/b',
                            '//article/article/section[2]/div/div[1]/ul/li[6]/p/b',
                            '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(7) p b'
                        ],
                        "Localização": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(1) p b',
                        "Ano do Modelo": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(2) p b',
                        "KM": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(3) p b',
                        "Transmissão": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(4) p b',
                        "Combustível": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(5) p b',
                        "Código Fipe": ['#version-price-fipe > tr:nth-child(1) > td:nth-child(4) > p',
                                        "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(4) > p"
                        ],
                        "Fipe": [
                            "#version-price-fipe > tr:nth-child(1) > td:nth-child(5) > p > b",
                            "//*[@id='mdl-7175874']/article/section[2]/div/div[4]/div/div[2]/span/span/h2/b",
                            "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(5) > p > b"
                        ],
                        "Anunciante": '#aside-init > div.column.spacing-1x.space-between.w100.style-module__Z2BY8a__container > span > span.wrap.space-between.style-module__Z2BY8a__nameContainer > a > span > h2 > b'
                    }

                    resultados = await asyncio.gather(
                        *[extrair_com_multiplos_seletores(pagina, v) if isinstance(v, list) else extrair_elemento(pagina, v)
                          for v in seletores.values()],
                        return_exceptions=True
                    )

                    dados = dict(zip(seletores.keys(), [r if not isinstance(r, Exception) else "N/A" for r in resultados]))
                    dados.update({"Link": link, "Cidade": "Desconhecido"})

                    # Processar preço
                    if dados["Preço"] != "N/A":
                        try:
                            dados["Preço"] = float(dados["Preço"].replace("R$", "").replace(".", "").replace(",", "."))
                        except ValueError:
                            logging.warning(f"Preço inválido: {dados['Preço']}")
                            dados["Preço"] = "N/A"

                    # Processar localização e cidade
                    if dados["Localização"] != "N/A" and " - " in dados["Localização"]:
                        dados["Cidade"] = dados["Localização"].split(" - ")[0]

                    # Processar ano
                    if dados["Ano do Modelo"] != "N/A":
                        try:
                            dados["Ano do Modelo"] = int(dados["Ano do Modelo"].split("/")[-1])
                        except ValueError:
                            logging.warning(f"Ano inválido: {dados['Ano do Modelo']}")
                            dados["Ano do Modelo"] = "N/A"

                    return dados
                except PlaywrightTimeoutError:
                    logging.warning(f"Tentativa {attempt + 1} falhou para {link}: Timeout")
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                    continue
                except Exception as e:
                    logging.error(f"Tentativa {attempt + 1} falhou para {link}: {e}")
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                    continue
            return None
        finally:
            if pagina:
                await pagina.close()

async def processar_links(links, max_concurrent=MAX_CONCURRENT):
    """Processa links em lotes com checkpointing."""
    start_time = time()
    dados_coletados = []
    processed_links = set()

    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            dados_coletados = pd.read_pickle(ARQUIVO_CHECKPOINT).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} links processados, {len(links)} restantes.")
        except Exception as e:
            logging.error(f"Erro ao carregar checkpoint: {e}")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(max_concurrent)
        for i in range(0, len(links), max_concurrent):
            if i > 0 and i % (max_concurrent * 10) == 0:
                await navegador.close()
                navegador = await p.chromium.launch(headless=True)
                logging.info("Navegador reiniciado para evitar vazamentos de memória.")

            batch = links[i:i + max_concurrent]
            async with (await navegador.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )) as contexto:
                tarefas = [extracao_dados(contexto, link, semaphore) for link in batch]
                for tarefa in tqdm(asyncio.as_completed(tarefas), total=len(tarefas), desc=f"Lote {i//max_concurrent + 1}"):
                    try:
                        resultado = await tarefa
                        if resultado:
                            dados_coletados.append(resultado)
                            if len(dados_coletados) % 100 == 0:
                                pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                                logging.info(f"Checkpoint salvo: {len(dados_coletados)} links.")
                    except Exception as e:
                        logging.error(f"Erro na tarefa do lote {i//max_concurrent + 1}: {e}")
                await asyncio.sleep(2)

        await navegador.close()

    elapsed_time = time() - start_time
    success_rate = len(dados_coletados) / (len(links) + len(dados_coletados)) * 100 if links or dados_coletados else 0
    logging.info(f"Processamento concluído em {elapsed_time:.2f}s. Taxa de sucesso: {success_rate:.2f}%")
    return dados_coletados

async def salvar_dados(dados_coletados):
    """Salva dados em PKL e Excel de forma incremental."""
    if not dados_coletados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados_coletados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"Dados salvos em '{ARQUIVO_PKL_DADOS}' ({len(df)} registros).")

    try:
        df_existente = pd.DataFrame()
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine='openpyxl')
        df_final = pd.concat([df_existente, df], ignore_index=True)
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine='openpyxl')
        logging.info(f"Dados salvos em '{ARQUIVO_EXCEL_DADOS}' ({len(df_final)} registros).")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel '{ARQUIVO_EXCEL_DADOS}': {e}")

async def main():
    """Função principal."""
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados_coletados = await processar_links(links)
    await salvar_dados(dados_coletados)

if __name__ == "__main__":
    asyncio.run(main())