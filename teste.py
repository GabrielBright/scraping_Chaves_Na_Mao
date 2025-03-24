import pandas as pd
import os
from playwright.async_api import async_playwright
import asyncio
import sys
import logging
from tqdm import tqdm
from urllib.parse import urlparse

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_carros.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

async def carregar_links():
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS, usecols=["Link"], engine="openpyxl")
    links = [link for link in df['Link'].dropna().unique().tolist() if is_valid_url(link)]
    logging.info(f"Carregados {len(links)} links únicos válidos.")
    return links

async def extrair_elemento(pagina, xpath, index=0, default="N/A"):
    try:
        elementos = pagina.locator(xpath)
        if await elementos.count() > index:
            return (await elementos.nth(index).inner_text()).strip()
        return default
    except Exception:
        return default

async def extracaoDados(contexto, link, semaphore, retries=3):
    async with semaphore:
        pagina = await contexto.new_page()
        for attempt in range(retries):
            try:
                response = await pagina.goto(link, timeout=42000)  # Reduzido para 10s
                if response.status != 200:
                    logging.warning(f"Status {response.status} em {link}.")
                    return None
                await pagina.wait_for_selector('//article//section[2]', timeout=42000)
                resultados = await asyncio.gather(
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//div//span//p//b', 0),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//div//span//p//b', 1),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//div//span//p//small'),
                    extrair_elemento(pagina, '//article/article/section[2]/div/div[1]/ul/li[7]/p/b'),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[3]//p//b'),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[4]//p//b'),
                    extrair_elemento(pagina, '//*[@id="version-price-fipe"]/tr[1]'),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[2]//p//b'),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[5]//p//b'),
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[1]//p//b'),
                    extrair_elemento(pagina, '//*[@id="aside-init"]/div[2]/span'),
                )
                dados = {
                    "Modelo": resultados[0], "Preço": resultados[1], "Versão": resultados[2],
                    "Cor": resultados[3], "KM": resultados[4], "Transmissão": resultados[5],
                    "Fipe": resultados[6], "Ano do Modelo": resultados[7], "Combustível": resultados[8],
                    "Localização": resultados[9], "Anunciante": resultados[10], "Cidade": "Desconhecido",
                    "Link": link
                }
                if " - " in dados["Localização"]:
                    dados["Cidade"] = dados["Localização"].split(" - ")[0]
                return dados
            except Exception as e:
                if attempt < retries - 1:
                    delay = 2 ** attempt
                    logging.warning(f"Tentativa {attempt + 1} falhou para {link}. Tentando novamente após {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Erro em {link} após {retries} tentativas: {e}")
                    return None
            finally:
                await pagina.close()

async def processar_links(links, max_concurrent=22): 
    dados_coletados = []
    falhas = 0
    semaphore = asyncio.Semaphore(max_concurrent)
    if os.path.exists(ARQUIVO_CHECKPOINT):
        with open(ARQUIVO_CHECKPOINT, 'rb') as f:
            dados_coletados = pd.read_pickle(f).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} links já processados, {len(links)} restantes.")
    
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        contexto = await navegador.new_context()
        with tqdm(total=len(links), desc="Processando links") as pbar:
            tarefas = [extracaoDados(contexto, link, semaphore) for link in links]
            for tarefa in asyncio.as_completed(tarefas):
                resultado = await tarefa
                if resultado:
                    dados_coletados.append(resultado)
                else:
                    falhas += 1
                pbar.update(1)
                if len(dados_coletados) % 200 == 0:
                    pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                    logging.info(f"Checkpoint salvo com {len(dados_coletados)} links. Falhas até agora: {falhas}")
                    await asyncio.sleep(0.2)  # Pausa de 0,2s a cada 50 links
        await contexto.close()
        await navegador.close()
    logging.info(f"Processamento concluído. Total de falhas: {falhas}")
    return dados_coletados

async def salvar_dados(dados_coletados):
    if not dados_coletados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados_coletados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    await asyncio.to_thread(df.to_excel, ARQUIVO_EXCEL_DADOS, index=False)
    logging.info(f"Finalizado: {len(df)} registros salvos. Sucessos: {len(df)}, Falhas: {45877 - len(df)}.")

async def main():
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados_coletados = await processar_links(links, max_concurrent=22)
    await salvar_dados(dados_coletados)

if __name__ == "__main__":
    asyncio.run(main())