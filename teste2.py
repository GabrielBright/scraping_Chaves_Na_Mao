import pandas as pd
import os
import asyncio
import sys
import logging
import re
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
TIMEOUT = 30_000
RETRIES = 3
MAX_CONCURRENT = 15

SELETORES_FIPE = {
    "codigo_fipe": [
        "#version-price-fipe > tr:nth-child(1) > td:nth-child(4) > p",
        "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(4) > p",
        "//table[@id='version-price-fipe']//td[position()=4]/p",
    ],
    "preco_fipe": [
        "#version-price-fipe > tr:nth-child(1) > td:nth-child(5) > p > b",
        "#version-price-fipe > tr.versionTemplate-module__qVM2Bq__highlighted > td:nth-child(5) > p > b",
        "//article/section[2]/div/div[3]/div/div[2]/span/span/h2/b",
        "//article/section[2]/div/div[3]/div/div[1]/span/span/h3/b",
    ]
}

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
        logging.info(f"Carregados {len(links)} links únicos.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar links: {e}")
        return []

async def extrair_por_seletores(pagina, seletores, default="N/A"):
    for seletor in seletores:
        try:
            is_xpath = seletor.strip().startswith("//") or seletor.strip().startswith("/html")
            locator = pagina.locator(f"xpath={seletor}" if is_xpath else seletor).first
            if await locator.count() == 0:
                continue
            try:
                await locator.scroll_into_view_if_needed()
                try:
                    texto = (await locator.inner_text(timeout=TIMEOUT)).strip()
                except:
                    texto = (await locator.text_content()).strip()
                if texto:
                    return texto
            except:
                continue
        except:
            continue
    return default

async def extrair_cor_com_validacao(pagina, seletores, default="N/A"):
    for seletor in seletores:
        try:
            is_xpath = seletor.strip().startswith("//") or seletor.strip().startswith("/html")
            locator = pagina.locator(f"xpath={seletor}" if is_xpath else seletor).first
            if await locator.count() == 0:
                continue
            try:
                await locator.scroll_into_view_if_needed()
                await locator.wait_for(state="visible", timeout=TIMEOUT)
                try:
                    texto = (await locator.inner_text()).strip()
                except:
                    texto = (await locator.text_content()).strip()
                if texto and not texto.isdigit():
                    return texto
            except:
                continue
        except:
            continue
    return default

async def extracao_dados(contexto, link, semaphore):
    async with semaphore:
        pagina = None
        try:
            pagina = await contexto.new_page()
            for attempt in range(RETRIES):
                try:
                    response = await pagina.goto(link, timeout=TIMEOUT, wait_until='domcontentloaded')
                    if response and response.status != 200:
                        return None

                    for _ in range(5):
                        if await pagina.locator("#version-price-fipe").count() > 0:
                            break
                        await pagina.evaluate("window.scrollBy(0, 800)")
                        await asyncio.sleep(0.5)

                    seletores = {
                        "Modelo": '.style-module__icNBzq__mainSection .column.spacing-2x div span p b',
                        "Versão": '.style-module__icNBzq__mainSection .column.spacing-2x div span p small',
                        "Preço": '.style-module__icNBzq__mainSection .column.spacing-2x div div span p b',
                        "Localização": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(1) p b',
                        "Ano do Modelo": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(2) p b',
                        "KM": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(3) p b',
                        "Transmissão": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(4) p b',
                        "Combustível": '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(5) p b',
                        "Anunciante": '#aside-init .style-module__Z2BY8a__nameContainer a span h2 b'
                    }

                    resultados = await asyncio.gather(
                        *[extrair_por_seletores(pagina, [v]) for v in seletores.values()],
                        return_exceptions=True
                    )

                    dados = dict(zip(seletores.keys(), [r if not isinstance(r, Exception) else "N/A" for r in resultados]))
                    dados["Link"] = link
                    dados["Cidade"] = "Desconhecido"

                    dados["Cor"] = await extrair_cor_com_validacao(
                        pagina,
                        [
                            '//li[contains(text(), "Cor")]/p/b',
                            '//article/article/section[2]/div/div[1]/ul/li[7]/p/b',
                            '//article/article/section[2]/div/div[1]/ul/li[6]/p/b',
                            '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(7) p b'
                        ]
                    )

                    dados["Código Fipe"] = await extrair_por_seletores(pagina, SELETORES_FIPE["codigo_fipe"])
                    dados["Fipe"] = await extrair_por_seletores(pagina, SELETORES_FIPE["preco_fipe"])

                    if dados["Preço"] != "N/A":
                        try:
                            dados["Preço"] = float(dados["Preço"].replace("R$", "").replace(".", "").replace(",", "."))
                        except:
                            dados["Preço"] = "N/A"

                    if dados["Localização"] != "N/A" and " - " in dados["Localização"]:
                        dados["Cidade"] = dados["Localização"].split(" - ")[0]

                    if dados["Ano do Modelo"] != "N/A":
                        try:
                            dados["Ano do Modelo"] = int(dados["Ano do Modelo"].split("/")[-1])
                        except:
                            dados["Ano do Modelo"] = "N/A"

                    return dados
                except:
                    await asyncio.sleep(2)
            return None
        finally:
            if pagina:
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
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} processados, {len(links)} restantes.")
        except Exception as e:
            logging.error(f"Erro ao carregar checkpoint: {e}")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        contexto = await navegador.new_context(user_agent="Mozilla/5.0 Chrome/120.0")
        semaphore = asyncio.Semaphore(max_concurrent)

        for i in range(0, len(links), max_concurrent):
            batch = links[i:i + max_concurrent]
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
                    logging.error(f"Erro em tarefa do lote {i//max_concurrent + 1}: {e}")
            await asyncio.sleep(1)

        await contexto.close()
        await navegador.close()

    elapsed = time() - start_time
    logging.info(f"Finalizado em {elapsed:.2f}s. Total: {len(dados_coletados)} registros.")
    return dados_coletados

async def salvar_dados(dados):
    if not dados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"Dados salvos em {ARQUIVO_PKL_DADOS}")

    try:
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine='openpyxl')
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine='openpyxl')
        logging.info(f"Excel salvo em {ARQUIVO_EXCEL_DADOS}")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel: {e}")

async def main():
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados = await processar_links(links)
    await salvar_dados(dados)

if __name__ == "__main__":
    asyncio.run(main())