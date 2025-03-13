from playwright.sync_api import sync_playwright
import pandas as pd
import time
import os

CIDADES = [
    ("São José do Rio Preto", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-do-rio-preto/", 286),
    ("Botucatu", "https://www.chavesnamao.com.br/carros-usados/sp-botucatu/", 292),
    ("Mogi das Cruzes", "https://www.chavesnamao.com.br/carros-usados/sp-mogi-das-cruzes/", 316),
    ("Limeira", "https://www.chavesnamao.com.br/carros-usados/sp-limeira/", 317),
    ("Jundiaí","https://www.chavesnamao.com.br/carros-usados/sp-jundiai/", 363),
    ("Guarulhos","https://www.chavesnamao.com.br/carros-usados/sp-guarulhos/", 378),
    ("Santo André", "https://www.chavesnamao.com.br/carros-usados/sp-santo-andre/", 403),
    ("São Bernado do Campo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-bernardo-do-campo/", 543),
    ("Piracicaba", "https://www.chavesnamao.com.br/carros-usados/sp-piracicaba/", 549),
    ("Santos", "https://www.chavesnamao.com.br/carros-usados/sp-santos/", 619),
    ("Osasco", "https://www.chavesnamao.com.br/carros-usados/sp-osasco/", 682),
    ("Ribeirão Preto", "https://www.chavesnamao.com.br/carros-usados/sp-ribeirao-preto/", 981),
    ("São José Dos Campos", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-dos-campos/", 997),
    ("Sorocaba", "https://www.chavesnamao.com.br/carros-usados/sp-sorocaba/", 1009),
    ('Campinas', "https://www.chavesnamao.com.br/carros-usados/sp-campinas/", 2490),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/", 3350),
    ("Porto Alegre", "https://www.chavesnamao.com.br/carros-usados/rs-porto-alegre/", 1808),
    ("FIAT", "https://www.chavesnamao.com.br/carros/pr-curitiba/fiat/?filtro=cid:[9668],amin:2002,amax:0,or:4", 1525),
    ("GM", "https://www.chavesnamao.com.br/carros/pr-curitiba/chevrolet/?filtro=cid:[9668],amin:2002,amax:0,or:4", 1822),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/pr-curitiba/hyundai/2002/", 602),
    ("U", "https://www.chavesnamao.com.br/carros-usados/mg-uberlandia/?filtro=cid:[4047+1090+4051+9758+8747+9756+6793+6796+6794],amin:2002,amax:0", 268),
    ("D", "https://www.chavesnamao.com.br/carros-usados/pr-diamante-do-sul/?filtro=cid:[9103+6030+9848+4163+9099+9094+7650+3047+6912],amin:2002,amax:0", 303),
    ("E", "https://www.chavesnamao.com.br/carros-usados/sp-engenheiro-coelho/?filtro=cid:[9114+3086+7700+9113+7697+7696+7667+7675+9124],amin:2002,amax:0", 395),
    ("A", "https://www.chavesnamao.com.br/carros-usados/pa-ananindeua/?filtro=cid:[8907+4273+8886+5789+2069+2065+8883+8913+8912],amin:2002,amax:0", 572),
    ("I", "https://www.chavesnamao.com.br/carros-usados/sp-itapolis/?filtro=cid:[6196+9250+8509+9260+3229+8488+6943+9216+8507],amin:2002,amax:0", 669),
    ("T", "https://www.chavesnamao.com.br/carros-usados/sc-timbo/?filtro=cid:[9707+8733+8742+4263+8227+6754+6770+5721+9726],amin:2002,amax:0", 670),
    ("F", "https://www.chavesnamao.com.br/carros-usados/sp-ferraz-de-vasconcelos/?filtro=cid:[7717+7710+3128+6091+6068+582+9144+1347+8452],amin:2002,amax:0", 999),
    ("N", "https://www.chavesnamao.com.br/carros-usados/rj-nilopolis/?filtro=cid:[3480+6368+9398+9415+8571+7221+6989+6994+7918],amin:2002,amax:0", 1015),
    ("V", "https://www.chavesnamao.com.br/carros-usados/rj-volta-redonda/?filtro=cid:[9775+8280+8294+1116+9783+2044+9768+2048+4511],amin:2002,amax:0", 1143),
    ("M", "https://www.chavesnamao.com.br/carros-usados/mg-montes-claros/?filtro=cid:[4677+6973+9348+9355+243+109+9354+9369+6308],amin:2002,amax:0", 1369),
    ("L", "https://www.chavesnamao.com.br/carros-usados/mt-lucas-do-rio-verde/?filtro=cid:[1925+9315+9316+789+7823+8525+9321+9317+6268],amin:2002,amax:0", 1484),
    ("G", "https://www.chavesnamao.com.br/carros-usados/sc-garuva/?filtro=cid:[1879+3169+9182+9177+8472+7748+6131+9183+2174],amin:2002,amax:0", 1558),
    ("J", "https://www.chavesnamao.com.br/carros-usados/pe-jaboatao-dos-guararapes/?filtro=cid:[9265+1451+9285+9267+4964+8518+3315+9295+8521],amin:2002,amax:0", 2500),
    ("P", "https://www.chavesnamao.com.br/carros-usados/rs-pelotas/?filtro=cid:[9532+9527+8589+7953+9501+6445+9493+6471+7994],amin:2002,amax:0", 3739),
    ("S", "https://www.chavesnamao.com.br/carros-usados/sp-santos/?filtro=cid:[9625+988+8700+9640+7069+9660],amin:2002,amax:0", 4618),
    ("C", "https://www.chavesnamao.com.br/carros-usados/mt-cuiaba/?filtro=cid:[8430+5964+7533+5941+4141+7566+2980+9025],amin:2002,amax:0", 4970),
    ("R", "https://www.chavesnamao.com.br/carros-usados/sp-registro/?filtro=cid:[9559+7042+7035+2298+8650+9566+5406+9560+7043],amin:2002,amax:0", 4987),
    ("B", "https://www.chavesnamao.com.br/carros-usados/sp-botucatu/?filtro=cid:[7466+2762+4565+8390+8961+8377+8357+1778+2754],amin:2002,amax:0", 5076),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?filtro=amin:2002,amax:0", 10503),
    ("Outros", "https://www.chavesnamao.com.br/carros/pr-curitiba/fiat-palio/?filtro=cid:[9668],amin:2002,amax:0,or:4", 50),
    ("O", "https://www.chavesnamao.com.br/carros-usados/rs-osorio/?filtro=cid:[5362+9435+3507],amin:2002,amax:0", 50),
]

LIMITE_MAXIMO = 5200
ARQUIVO_EXCEL = "dados_carros_cidades.xlsx"

def salvar_progresso(dados):
    df = pd.DataFrame(dados)
    if os.path.exists(ARQUIVO_EXCEL):
        df_existente = pd.read_excel(ARQUIVO_EXCEL)
        df = pd.concat([df_existente, df], ignore_index=True)
    df.to_excel(ARQUIVO_EXCEL, index=False)
    print(f"Progresso salvo em '{ARQUIVO_EXCEL}'")

def coletar_dados_veiculo(elemento):
    try:
        descricao_completa = elemento.inner_text()
        partes = descricao_completa.split("\n")

        modelo = partes[0] if len(partes) > 0 else "Não informado"
        versao = partes[1] if len(partes) > 1 else "Não informado"
        preco = next((p for p in partes if "R$" in p), "Não informado")  
        ano = next((p for p in partes if p.isdigit() and len(p) == 4), "Não informado")  
        kilometragem = next((p for p in partes if "km" in p), "Não informado")  
        localizacao = partes[-1] if len(partes) > 2 else "Não informado"  

        return {
            "Modelo": modelo,
            "Versão": versao,
            "Preço": preco,
            "Ano": ano,
            "Kilometragem": kilometragem,
            "Localização": localizacao
        }
    except Exception as e:
        print(f"Erro ao coletar dados do veículo: {e}")
        return {
            "Modelo": "Erro",
            "Versão": "Erro",
            "Preço": "Erro",
            "Ano": "Erro",
            "Kilometragem": "Erro",
            "Localização": "Erro"
        }

def carregar_todos_os_itens(pagina, limite_itens):
    dados_veiculos = []
    links_coletados = set()
    tentativas_sem_novos = 0

    while len(dados_veiculos) < limite_itens and tentativas_sem_novos < 10:
        if len(dados_veiculos) >= LIMITE_MAXIMO:
            print(f" Limite de {LIMITE_MAXIMO} itens atingido. Parando a extração.")
            break

        try:
            elementos_itens = pagina.locator('//div[starts-with(@id, "vc-")]/a/span[2]/span')
            elementos_links = pagina.locator('//div[starts-with(@id, "vc-")]/a')
            
            total_itens = elementos_itens.count()
            if total_itens == len(dados_veiculos):
                tentativas_sem_novos += 1
            else:
                tentativas_sem_novos = 0

            for i in range(total_itens):
                if len(dados_veiculos) >= LIMITE_MAXIMO:
                    break

                try:
                    elemento = elementos_itens.nth(i)
                    link_elemento = elementos_links.nth(i)

                    descricao = elemento.inner_text(timeout=80000)
                    link = link_elemento.get_attribute("href", timeout=80000)

                    if link and link in links_coletados:
                        continue

                    if descricao and link:
                        dados_veiculo = coletar_dados_veiculo(elemento)
                        dados_veiculo["Link"] = "https://www.chavesnamao.com.br" + link
                        dados_veiculos.append(dados_veiculo)
                        links_coletados.add(link)

                except Exception as e:
                    print(f"Erro ao processar um item: {e}")
                    continue

            pagina.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(8)

        except Exception as e:
            print(f"Erro ao carregar itens: {e}")
            break

    return dados_veiculos

def extrair_dados_cidade(pagina, cidade, url, limite):
    print(f" Extraindo dados de {cidade}...")
    pagina.goto(url, timeout=80000)
    pagina.wait_for_load_state('networkidle', timeout=60000)

    veiculos = carregar_todos_os_itens(pagina, limite)
    return [{"Cidade": cidade, **veiculo} for veiculo in veiculos]

with sync_playwright() as p:
    navegador = p.chromium.launch(headless=False)
    pagina = navegador.new_page()

    for cidade, url, limite in CIDADES:
        dados = extrair_dados_cidade(pagina, cidade, url, limite)
        salvar_progresso(dados)

    navegador.close()
    print("Coleta finalizada!")

