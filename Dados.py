import pandas as pd
import os
import asyncio
import sys
import logging
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import time

# Configurações
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Arquivos
ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_carros.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

# Constantes
TIMEOUT = 30000  # 30 segundos
RETRIES = 3
MAX_CONCURRENT = 15  # Reduzido para evitar sobrecarga

async def carregar_links():
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS)
        if "Link" not in df.columns:
            logging.error(f"Coluna 'Link' não encontrada em {ARQUIVO_EXCEL_LINKS}.")
            return []
        links = df['Link'].dropna().unique().tolist()
        logging.info(f"Carregados {len(links)} links únicos do arquivo Excel.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar {ARQUIVO_EXCEL_LINKS}: {e}", exc_info=True)
        return []

async def extrair_elemento(pagina, seletor, default="N/A"):
    try:
        elemento = pagina.locator(seletor)
        count = await elemento.count()
        if count > 0:
            await elemento.first.wait_for(timeout=TIMEOUT)
            texto = (await elemento.first.inner_text()).strip()
            return texto if texto else default
        return default
    except Exception as e:
        logging.debug(f"Erro ao extrair com seletor {seletor}: {e}")
        return default

async def extrair_com_multiplos_seletores(pagina, seletores, default="N/A", link=""):
    for seletor in seletores:
        valor = await extrair_elemento(pagina, seletor)
        if valor != "N/A":
            return valor
    return default

async def extracaoDados(contexto, link, semaphore):
    async with semaphore:
        pagina = None
        try:
            pagina = await contexto.new_page()
            logging.info(f"Acessando {link}")
            for attempt in range(RETRIES):
                try:
                    response = await pagina.goto(link, timeout=TIMEOUT)
                    if response and response.status != 200:
                        logging.warning(f"Status {response.status} em {link}. Possível bloqueio.")
                        return None
                    await pagina.wait_for_load_state('domcontentloaded', timeout=TIMEOUT)
                    await pagina.wait_for_timeout(1000)

                    # Scroll forçado para carregar a tabela FIPE
                    for _ in range(10):
                        if await pagina.locator("#version-price-fipe").count() > 0:
                            break
                        await pagina.mouse.wheel(0, 800)
                        await pagina.wait_for_timeout(1000)

                    cor_seletores = [
                        '//li[contains(text(), "Cor")]/p/b',
                        '//article/article/section[2]/div/div[1]/ul/li[7]/p/b',
                        '//article/article/section[2]/div/div[1]/ul/li[6]/p/b',
                        '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(7) p b'
                    ]
                    preco_fipe_seletores = [
                        "#version-price-fipe > tr:nth-child(1) > td:nth-child(3) > p > b",
                        "//article/section[2]/div/div[4]/div/div[2]/span/span/h2/b"
                    ]
                    resultados = await asyncio.gather(
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x div span p b'),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x div span p small'),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x div div span p b'),
                        extrair_com_multiplos_seletores(pagina, cor_seletores, link=link),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(1) p b'),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(2) p b'),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(3) p b'),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(4) p b'),
                        extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(5) p b'),
                        extrair_elemento(pagina, '#version-price-fipe > tr:nth-child(1) > td:nth-child(2) > p'),
                        extrair_com_multiplos_seletores(pagina, preco_fipe_seletores, link=link),
                        extrair_elemento(pagina, '#aside-init .style-module__Z2BY8a__container .style-module__Z2BY8a__nameContainer a span h2 b'),
                        return_exceptions=True
                    )
                    dados = {
                        "Modelo": resultados[0] if not isinstance(resultados[0], Exception) else "N/A",
                        "Versão": resultados[1] if not isinstance(resultados[1], Exception) else "N/A",
                        "Preço": resultados[2] if not isinstance(resultados[2], Exception) else "N/A",
                        "Cor": resultados[3] if not isinstance(resultados[3], Exception) else "N/A",
                        "KM": resultados[6] if not isinstance(resultados[6], Exception) else "N/A",
                        "Transmissão": resultados[7] if not isinstance(resultados[7], Exception) else "N/A",
                        "Fipe": resultados[10] if not isinstance(resultados[10], Exception) else "N/A",
                        "Código Fipe": resultados[9] if not isinstance(resultados[9], Exception) else "N/A",
                        "Ano do Modelo": resultados[5] if not isinstance(resultados[5], Exception) else "N/A",
                        "Combustível": resultados[8] if not isinstance(resultados[8], Exception) else "N/A",
                        "Localização": resultados[4] if not isinstance(resultados[4], Exception) else "N/A",
                        "Anunciante": resultados[11] if not isinstance(resultados[11], Exception) else "N/A",
                        "Cidade": "Desconhecido",
                        "Link": link
                    }

                    if dados["Preço"] != "N/A":
                        try:
                            dados["Preço"] = float(dados["Preço"].replace("R$", "").replace(".", "").replace(",", "."))
                        except ValueError:
                            logging.warning(f"Preço inválido para {link}: {dados['Preço']}")
                            dados["Preço"] = "N/A"

                    if dados["Localização"] != "N/A" and " - " in dados["Localização"]:
                        dados["Cidade"] = dados["Localização"].split(" - ")[0]

                    if dados["Ano do Modelo"] != "N/A":
                        try:
                            ano = int(dados["Ano do Modelo"].split("/")[-1])
                            dados["Ano do Modelo"] = ano
                        except ValueError:
                            logging.warning(f"Ano inválido para {link}: {dados['Ano do Modelo']}")
                            dados["Ano do Modelo"] = "N/A"

                    return dados
                except PlaywrightTimeoutError as e:
                    logging.warning(f"Tentativa {attempt + 1} falhou para {link}: Timeout - {str(e)}")
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(1)
                    continue
                except Exception as e:
                    logging.error(f"Tentativa {attempt + 1} falhou para {link}: {str(e)}", exc_info=True)
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(1)
                    continue
            return None
        except Exception as e:
            logging.error(f"Erro ao criar página para {link}: {str(e)}", exc_info=True)
            return None
        finally:
            if pagina:
                await pagina.close()

async def processar_links(links, max_concurrent=MAX_CONCURRENT):
    start_time = time.time()
    dados_coletados = []
    semaphore = asyncio.Semaphore(max_concurrent)
    if os.path.exists(ARQUIVO_CHECKPOINT):
        with open(ARQUIVO_CHECKPOINT, 'rb') as f:
            dados_coletados = pd.read_pickle(f).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} links já processados, {len(links)} restantes.")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        for i in range(0, len(links), max_concurrent):
            if i % (max_concurrent * 15) == 0 and i > 0:  # Reinicia a cada 15 lotes
                await navegador.close()
                navegador = await p.chromium.launch(headless=True)
            batch = links[i:i + max_concurrent]
            contexto = await navegador.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            try:
                tarefas = [extracaoDados(contexto, link, semaphore) for link in batch]
                for tarefa in tqdm(asyncio.as_completed(tarefas), total=len(batch), desc="Processando lote"):
                    resultado = await tarefa
                    if resultado:
                        dados_coletados.append(resultado)
                        if len(dados_coletados) % 500 == 0:  # Checkpoint a cada 500 links
                            pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                            logging.info(f"Checkpoint salvo com {len(dados_coletados)} links.")
                await asyncio.sleep(1)  # Pausa maior entre lotes
            except Exception as e:
                logging.error(f"Erro no processamento do lote {i//max_concurrent + 1}: {str(e)}", exc_info=True)
            finally:
                await contexto.close()
        await navegador.close()

    elapsed_time = time.time() - start_time
    success_rate = len(dados_coletados) / len(links) * 100 if links else 0
    logging.info(f"Processamento concluído em {elapsed_time:.2f}s. Taxa de sucesso: {success_rate:.2f}%")
    return dados_coletados

async def salvar_dados(dados_coletados):
    if not dados_coletados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados_coletados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"Dados salvos em '{ARQUIVO_PKL_DADOS}' ({len(df)} registros).")

    # Exportação incremental para Excel
    try:
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            # Carregar dados existentes
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine='openpyxl')
            # Concatenar novos dados
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df
        # Salvar o arquivo Excel
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine='openpyxl')
        logging.info(f"Dados salvos em '{ARQUIVO_EXCEL_DADOS}' ({len(df_final)} registros).")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel '{ARQUIVO_EXCEL_DADOS}': {str(e)}", exc_info=True)

async def main():
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados_coletados = await processar_links(links)
    await salvar_dados(dados_coletados)

if __name__ == "__main__":
    asyncio.run(main())