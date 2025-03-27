import pandas as pd
import os
from playwright.async_api import async_playwright
import asyncio
import sys
import logging
from tqdm import tqdm

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Arquivos
ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_motos.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

async def carregar_links():
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS, usecols=["Link"])
        links = df['Link'].dropna().unique().tolist()
        logging.info(f"Carregados {len(links)} links únicos do arquivo Excel.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar {ARQUIVO_EXCEL_LINKS}: {e}")
        return []

async def extrair_elemento(pagina, seletor, default="N/A"):
    try:
        elemento = pagina.locator(seletor)
        if await elemento.count() > 0:
            texto = (await elemento.first.inner_text()).strip()
            if texto:
                return texto
        return default
    except Exception:
        return default

async def extrair_com_multiplos_seletores(pagina, seletores, default="N/A", link=""):
    for seletor in seletores:
        valor = await extrair_elemento(pagina, seletor)
        if valor != "N/A":
            logging.debug(f"Cor extraída com sucesso de '{seletor}' para {link}: {valor}")
            return valor
        else:
            logging.debug(f"Cor não encontrada em '{seletor}' para {link}")
    logging.debug(f"Nenhum seletor de Cor funcionou para {link}, retornando {default}")
    return default

async def extracaoDados(contexto, link, semaphore, retries=2):
    async with semaphore:
        pagina = await contexto.new_page()
        logging.info(f"Acessando {link}")
        for attempt in range(retries):
            try:
                response = await pagina.goto(link, timeout=30000)
                if response.status != 200:
                    logging.warning(f"Status {response.status} em {link}. Possível bloqueio.")
                    return None
                await pagina.wait_for_load_state('domcontentloaded', timeout=30000)

                # Seletores para "Cor" (tentar 7 primeiro, depois 6)
                cor_seletores = [
                    '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(7) p b',
                    '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(6) p b'
                ]
                # Seletores para "Preço Fipe" (com ou sem <b>)
                preco_fipe_seletores = [
                    '#version-price-fipe tr:nth-child(1) td:nth-child(3) p b',
                    '#version-price-fipe tr:nth-child(1) td:nth-child(3) p'
                ]
                # Extração dos dados
                resultados = await asyncio.gather(
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x div span p b'),  # Modelo
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x div span p small'),  # Versão
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x div div span p b'),  # Preço
                    extrair_com_multiplos_seletores(pagina, cor_seletores, link=link),  # Cor
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(1) p b'),  # Localização
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(2) p b'),  # Ano do Modelo
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(3) p b'),  # KM
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(4) p b'),  # Transmissão
                    extrair_elemento(pagina, '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(5) p b'),  # Combustível
                    extrair_elemento(pagina, '#version-price-fipe tr:nth-child(1) td:nth-child(2) p'),  # Código Fipe
                    extrair_com_multiplos_seletores(pagina, preco_fipe_seletores, link=link),  # Preço Fipe
                    extrair_elemento(pagina, '#aside-init .style-module__Z2BY8a__container .style-module__Z2BY8a__nameContainer a span h2 b'),  # Anunciante
                )
                dados = {
                    "Modelo": resultados[0],
                    "Versão": resultados[1],
                    "Preço": resultados[2],
                    "Cor": resultados[3],
                    "KM": resultados[6],
                    "Transmissão": resultados[7],
                    "Fipe": resultados[10],  # Preço Fipe
                    "Código Fipe": resultados[9],  # Código Fipe separado
                    "Ano do Modelo": resultados[5],
                    "Combustível": resultados[8],
                    "Localização": resultados[4],
                    "Anunciante": resultados[11],
                    "Cidade": "Desconhecido",
                    "Link": link
                }
                if " - " in dados["Localização"]:
                    dados["Cidade"] = dados["Localização"].split(" - ")[0]
                return dados
            except Exception as e:
                if attempt < retries - 1:
                    logging.warning(f"Tentativa {attempt + 1} falhou para {link}. Tentando novamente após 2s...")
                    await asyncio.sleep(2)
                else:
                    logging.error(f"Erro em {link} após {retries} tentativas: {e}")
                    return None
            finally:
                await pagina.close()

async def processar_links(links, max_concurrent=25):
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
            batch = links[i:i + max_concurrent]
            contexto = await navegador.new_context()
            try:
                tarefas = [extracaoDados(contexto, link, semaphore) for link in batch]
                for tarefa in tqdm(asyncio.as_completed(tarefas), total=len(batch), desc="Processando lote"):
                    resultado = await tarefa
                    if resultado:
                        dados_coletados.append(resultado)
                        if len(dados_coletados) % 500 == 0:
                            pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                            logging.info(f"Checkpoint salvo com {len(dados_coletados)} links.")
            finally:
                await contexto.close()
        await navegador.close()

    return dados_coletados

async def salvar_dados(dados_coletados):
    if not dados_coletados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados_coletados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"Dados salvos em '{ARQUIVO_PKL_DADOS}' ({len(df)} registros).")
    await asyncio.to_thread(df.to_excel, ARQUIVO_EXCEL_DADOS, index=False)
    logging.info(f"Dados salvos em '{ARQUIVO_EXCEL_DADOS}' ({len(df)} registros).")

async def main():
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados_coletados = await processar_links(links)
    await salvar_dados(dados_coletados)

if __name__ == "__main__":
    asyncio.run(main())