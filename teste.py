import pandas as pd
import os
import asyncio
import sys
import logging
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import time
import random

# Configurações
sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Arquivos
ARQUIVO_EXCEL_LINKS = "teste_motos.xlsx"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint.pkl"

# Constantes
TIMEOUT = 30000  # 30 segundos
RETRIES = 3
MAX_CONCURRENT = 15

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

async def extrair_elemento(pagina, seletor):
    try:
        elemento = await pagina.locator(seletor).first.text_content()
        return elemento.strip() if elemento else "N/A"
    except Exception as e:
        logging.debug(f"Erro ao extrair com seletor {seletor}: {str(e)}")
        return "N/A"

async def extrair_com_multiplos_seletores(pagina, seletores, link=None):
    for seletor in seletores:
        try:
            count = await pagina.locator(seletor).count()
            if count > 0:
                elemento = await pagina.locator(seletor).first.text_content()
                if elemento:
                    logging.debug(f"Valor extraído com sucesso de '{seletor}' para {link}: {elemento.strip()}")
                    return elemento.strip()
        except Exception as e:
            logging.debug(f"Erro ao extrair com seletor {seletor}: {str(e)}")
    return "N/A"

async def extrair_fipe_dados(pagina, link):
    try:
        if "/moto/" in link:
            seletores_codigo = [
                'xpath=//table[@id="version-price-fipe"]/tbody/tr[1]/td[2]'
            ]
            seletores_preco = [
                'xpath=//table[@id="version-price-fipe"]/tbody/tr[1]/td[3]//b',
                'xpath=//div[contains(@class, "__priceBox")]//h3',
                'xpath=//div[contains(@class, "__boxFipe")]//h2//b'
            ]
        else:
            seletores_codigo = [
                'xpath=//table[@id="version-price-fipe"]//tr[contains(@class,"highlighted")]/td[4]//p'
            ]
            seletores_preco = [
                'xpath=//table[@id="version-price-fipe"]//tr[contains(@class,"highlighted")]/td[5]//p//b',
                'xpath=//div[contains(@class, "__boxFipe")]//h2//b',
                'xpath=//div[contains(@class, "__priceBox")]//h3'
            ]

        codigo_fipe = await extrair_com_multiplos_seletores(pagina, seletores_codigo, link=link)
        preco_fipe = await extrair_com_multiplos_seletores(pagina, seletores_preco, link=link)

        return codigo_fipe or "N/A", preco_fipe or "N/A"

    except Exception as e:
        logging.debug(f"Erro ao extrair FIPE em {link}: {str(e)}")
        return "N/A", "N/A"

async def extracaoDados(contexto, link, semaphore, RETRIES=3, TIMEOUT=30000):
    async with semaphore:
        pagina = None
        try:
            pagina = await contexto.new_page()
            logging.info(f"Acessando {link}")
            for attempt in range(RETRIES):
                try:
                    await pagina.set_extra_http_headers({
                        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                    })
                    response = await pagina.goto(link, timeout=TIMEOUT)
                    if response and response.status != 200:
                        logging.warning(f"Status {response.status} em {link}. Possível bloqueio.")
                        return None

                    await pagina.wait_for_load_state('networkidle', timeout=TIMEOUT)
                    await pagina.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await pagina.wait_for_timeout(15000)

                    captcha_text = await pagina.locator('text="Por favor, verifique que você não é um robô"').count()
                    if captcha_text > 0:
                        logging.warning(f"CAPTCHA detectado em {link}. Extração falhou.")
                        return None

                    cor_seletores = [
                        '.style-module__icNBzq__mainSection .column.spacing-2x ul li:nth-child(7) p b',
                        'xpath=//li[contains(text(), "Cor")]/p/b',
                        'xpath=//article//ul/li[7]/p/b',
                        'xpath=//article//ul/li[6]/p/b'
                    ]

                    # NOVO: Extrair código e preço FIPE com seletor atualizado
                    codigo_fipe, preco_fipe = await extrair_fipe_dados(pagina, link)

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
                        extrair_elemento(pagina, '#aside-init .style-module__Z2BY8a__container .style-module__Z2BY8a__nameContainer a span h2 b'),  # Anunciante
                        return_exceptions=True
                    )

                    dados = {
                        "Modelo": resultados[0] if not isinstance(resultados[0], Exception) else "N/A",
                        "Versão": resultados[1] if not isinstance(resultados[1], Exception) else "N/A",
                        "Preço": resultados[2] if not isinstance(resultados[2], Exception) else "N/A",
                        "Cor": resultados[3] if not isinstance(resultados[3], Exception) else "N/A",
                        "KM": resultados[6] if not isinstance(resultados[6], Exception) else "N/A",
                        "Transmissão": resultados[7] if not isinstance(resultados[7], Exception) else "N/A",
                        "Fipe": preco_fipe,
                        "Código Fipe": codigo_fipe,
                        "Ano do Modelo": resultados[5] if not isinstance(resultados[5], Exception) else "N/A",
                        "Combustível": resultados[8] if not isinstance(resultados[8], Exception) else "N/A",
                        "Localização": resultados[4] if not isinstance(resultados[4], Exception) else "N/A",
                        "Anunciante": resultados[9] if not isinstance(resultados[9], Exception) else "N/A",
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

                    if dados["Código Fipe"] == dados["Fipe"] and dados["Código Fipe"] != "N/A":
                        logging.warning(f"Ambiguidade: Código Fipe ({dados['Código Fipe']}) igual a Preço Fipe para {link}")
                        dados["Fipe"] = "N/A"

                    await asyncio.sleep(random.uniform(0.5, 1.5))
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
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} já processados, {len(links)} restantes.")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        for i in range(0, len(links), max_concurrent):
            if i % (max_concurrent * 15) == 0 and i > 0:
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
                        if len(dados_coletados) % 20 == 0:
                            pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                            logging.info(f"Checkpoint salvo com {len(dados_coletados)} registros.")
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Erro no processamento do lote {i//max_concurrent + 1}: {str(e)}", exc_info=True)
            finally:
                await contexto.close()
        await navegador.close()

    elapsed_time = time.time() - start_time
    success_rate = len(dados_coletados) / len(links) * 100 if links else 0
    logging.info(f"Concluído em {elapsed_time:.2f}s. Taxa de sucesso: {success_rate:.2f}%")
    return dados_coletados

async def salvar_dados(dados_coletados):
    if not dados_coletados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados_coletados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"Dados salvos em '{ARQUIVO_PKL_DADOS}' ({len(df)} registros).")

    try:
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine='openpyxl')
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine='openpyxl')
        logging.info(f"Dados salvos em '{ARQUIVO_EXCEL_DADOS}' ({len(df_final)} registros).")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel: {str(e)}", exc_info=True)

async def main():
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados_coletados = await processar_links(links)
    await salvar_dados(dados_coletados)

if __name__ == "__main__":
    asyncio.run(main())
