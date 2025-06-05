import pandas as pd
import os
import asyncio
import sys
import logging
import re
from tqdm import tqdm
from playwright.async_api import async_playwright
from time import time

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Entrada de dados
ARQUIVO_EXCEL_LINKS = "links_chaves_na_mao_motos.xlsx"

# Saída dos dados
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao_motos.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao_motos.xlsx"

# Dados salvos acada 500 dados coletados
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

# Limites
TIMEOUT = 30000
RETRIES = 3
MAX_CONCURRENT = 15

SELETORES_FIPE = {
    "codigo_fipe": [
        '//*[@id="version-price-fipe"]/tr[1]/td[2]/p',
        '//*[@id="version-price-fipe"]/tr[1]/td[2]',
        "#version-price-fipe > tr:nth-child(1) > td:nth-child(2) > p",
        "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(2) > p",
        "//table[@id='version-price-fipe']//td[position()=2]/p",
    ],
    "preco_fipe": [
        "//*[@id='version-price-fipe']/tr[1]/td[3]/p/b",
        "//article/section[2]/div/div[4]/div/div[2]/span/span/h2/b",
        "//article/section[2]/div/div[4]/div/div[1]/span/span/h3/b",
    ]
}

# Carrega os links da base de entrada
async def carregar_links():
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS)
        if "Link" not in df.columns:
            logging.error("Coluna 'Link' não encontrada.")
            return []
        links = df['Link'].dropna().unique().tolist()
        logging.info(f"{len(links)} links únicos carregados.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar links: {e}")
        return []

async def extrair_texto(pagina, seletores, default="N/A"):
    for seletor in seletores:
        try:
            is_xpath = seletor.strip().startswith("//")
            locator = pagina.locator(f"xpath={seletor}" if is_xpath else seletor).first
            if await locator.count() > 0:
                texto = await locator.text_content(timeout=TIMEOUT)
                if texto:
                    return texto.strip()
        except:
            continue
    return default

# Extração dos dados tecnicos 
async def extracao_dados(contexto, link, semaphore):
    async with semaphore:
        pagina = await contexto.new_page()
        try:
            for _ in range(RETRIES):
                try:
                    response = await pagina.goto(link, timeout=TIMEOUT, wait_until='domcontentloaded')
                    if not response or response.status != 200:
                        continue

                    # Scroll básico até a tabela aparecer
                    for _ in range(5):
                        if await pagina.locator("#version-price-fipe").count() > 0:
                            break
                        await pagina.evaluate("window.scrollBy(0, 800)")
                        await asyncio.sleep(0.4)

                    seletores = {
                        "Modelo": 'main article section.row div.column span p > b',
                        "Versão": 'main article section.row div.column span p > small',
                        "Preço": 'body > main > article > section.row.spacing-4x.space-between.style-module__vnSL7G__mainSection > div > div.column.spacing-2x > div > div > span > p > b',
                        "Localização": 'main article section.row div.column ul > li:nth-child(1) > p > b',
                        "Ano do Modelo": 'main article section.row div.column ul > li:nth-child(2) > p > b',
                        "KM": 'main article section.row div.column ul > li:nth-child(3) > p > b',
                        "Transmissão": 'main article section.row div.column ul > li:nth-child(4) > p > b',
                        "Combustível": 'main article section.row div.column ul > li:nth-child(5) > p > b',
                        "Anunciante": 'aside span span.wrap a span h2 > b'
                    }

                    dados = {}
                    for chave, seletor in seletores.items():
                        dados[chave] = await extrair_texto(pagina, [seletor])

                    dados["Link"] = link
                    dados["Cidade"] = "Desconhecido"

                    dados["Cor"] = await extrair_texto(pagina, [
                        '//li[contains(text(), "Cor")]/p/b',
                        '//article/article/section[2]/div/div[1]/ul/li[7]/p/b',
                        '//article/article/section[2]/div/div[1]/ul/li[6]/p/b',
                        'body > main > article > section.row.spacing-4x.space-between.style-module__vnSL7G__mainSection > div > div.column.spacing-2x > ul > li:nth-child(7) > p > b',
                        'body > main > article > section.row.spacing-4x.space-between.style-module__vnSL7G__mainSection > div > div.column.spacing-2x > ul > li:nth-child(6) > p > b'
                        '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(7) p b'
                    ])

                    dados["Código Fipe"] = await extrair_texto(pagina, SELETORES_FIPE["codigo_fipe"])
                    dados["Fipe"] = await extrair_texto(pagina, SELETORES_FIPE["preco_fipe"])

                    try:
                        dados["Preço"] = float(dados["Preço"].replace("R$", "").replace(".", "").replace(",", "."))
                    except:
                        dados["Preço"] = "N/A"

                    if " - " in dados.get("Localização", ""):
                        dados["Cidade"] = dados["Localização"].split(" - ")[0]

                    try:
                        dados["Ano do Modelo"] = int(dados["Ano do Modelo"].split("/")[-1])
                    except:
                        dados["Ano do Modelo"] = "N/A"

                    return dados
                except:
                    await asyncio.sleep(1)
            return None
        finally:
            await pagina.close()

async def processar_links(links, max_concurrent=MAX_CONCURRENT):
    start_time = time()
    dados_coletados = []
    processed_links = set()

    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            dados_coletados = pd.read_pickle(ARQUIVO_CHECKPOINT).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} prontos, {len(links)} restantes.")
        except Exception as e:
            logging.error(f"Erro ao carregar checkpoint: {e}")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        contexto = await navegador.new_context()
        semaphore = asyncio.Semaphore(max_concurrent)

        for i in range(0, len(links), max_concurrent):
            batch = links[i:i + max_concurrent]
            tarefas = [extracao_dados(contexto, link, semaphore) for link in batch]
            for tarefa in tqdm(asyncio.as_completed(tarefas), total=len(tarefas), desc=f"Lote {i//max_concurrent + 1}"):
                try:
                    resultado = await tarefa
                    if resultado:
                        dados_coletados.append(resultado)
                        if len(dados_coletados) % 500 == 0:
                            pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                            logging.info(f"{len(dados_coletados)} salvos no checkpoint.")
                except Exception as e:
                    logging.error(f"Erro ao processar link: {e}")
            await asyncio.sleep(0.5)

        await contexto.close()
        await navegador.close()

    logging.info(f"Finalizado em {time() - start_time:.2f}s com {len(dados_coletados)} registros.")
    return dados_coletados

async def salvar_dados(dados):
    if not dados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"PKL salvo: {ARQUIVO_PKL_DADOS}")

    try:
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine='openpyxl')
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine='openpyxl')
        logging.info(f"Excel salvo: {ARQUIVO_EXCEL_DADOS}")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel: {e}")

async def main():
    links = await carregar_links()
    if not links:
        return
    dados = await processar_links(links)
    await salvar_dados(dados)

if __name__ == "__main__":
    asyncio.run(main())
