import pandas as pd
import os
from playwright.async_api import async_playwright
import asyncio
import sys
import logging
from tqdm import tqdm

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Salvando Dados coletados em Excel
ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_carros.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

#Carregando os links coletados do arquivo links_chaves_na_mao_carros.xlsx para coletar os dados 
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

async def extrair_elemento(pagina, xpath, index=0, default="N/A"):
    try:
        elementos = pagina.locator(xpath)
        if await elementos.count() > index:
            return (await elementos.nth(index).inner_text()).strip()
        return default
    except Exception:
        return default

#Extrai os dados 
async def extracaoDados(contexto, link, semaphore, retries=2):
    async with semaphore:
        pagina = await contexto.new_page()
        logging.info(f"Acessando {link}")
        for attempt in range(retries):
            try:
                response = await pagina.goto(link, timeout=45000)
                if response.status != 200:
                    logging.warning(f"Status {response.status} em {link}. Possível bloqueio.")
                    return None
                #Tempo para não ocorrer algum erro
                await pagina.wait_for_load_state('domcontentloaded', timeout=45000)
                #xpath dos dados em sua pagina da web
                resultados = await asyncio.gather(
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//div//span//p//b', 0),  # Modelo
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//div//span//p//b', 1),  # Preço
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//div//span//p//small'),  # Versão
                    extrair_elemento(pagina, '//article/article/section[2]/div/div[1]/ul/li[7]/p/b'), # Cor
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[3]//p//b'),  # KM
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[4]//p//b'),  # Transmissão
                    extrair_elemento(pagina, '//*[@id="version-price-fipe"]/tr[1]'),  # Fipe
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[2]//p//b'),  # Ano do Modelo
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[5]//p//b'),  # Combustível
                    extrair_elemento(pagina, '//article//section[2]//div//div[1]//ul//li[1]//p//b'),  # Localização
                    extrair_elemento(pagina, '//*[@id="aside-init"]/div[2]/span'),  # Anunciante
                )
                #Separando em colunas
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
                    logging.warning(f"Tentativa {attempt + 1} falhou para {link}. Tentando novamente após 2s...")
                    await asyncio.sleep(2)
                else:
                    logging.error(f"Erro em {link} após {retries} tentativas: {e}")
                    return None
            finally:
                await pagina.close()

#Processamentos dos links de 15 em 15 links
async def processar_links(links, max_concurrent=25):
    dados_coletados = []
    semaphore = asyncio.Semaphore(max_concurrent)
    #Salva em checkpoints
    if os.path.exists(ARQUIVO_CHECKPOINT):
        with open(ARQUIVO_CHECKPOINT, 'rb') as f:
            dados_coletados = pd.read_pickle(f).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} links já processados, {len(links)} restantes.")

    #Abre o navegador do playright, está salvando a cada 500 coletados
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

#Salvando os dados salvos do checkpoint em um arquivo pkl
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
