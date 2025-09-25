# src/load_to_neo4j.py
import math
import os
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

# ====== CONFIG ======
# use exatamente o caminho que você passou:
xlsx_path = Path(r"E:\TCC\Interface\Projeto_Grafo_TCC\data\modelo_grafo_REDS_v2_backup_20250924_062234.xlsx")

# lê credenciais do .env na raiz do projeto
# (NEO4J_URI, NEO4J_USER, NEO4J_PASS)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASS", "senha-forte")

BATCH = 5000  # tamanho do lote de gravação

def is_ok(v):
    if v is None: return False
    if isinstance(v, float) and math.isnan(v): return False
    s = str(v).strip()
    return s != "" and s.lower() != "nan"

def to_int(v):
    try:
        return int(float(v))
    except:
        return None

def to_float(v):
    try:
        return float(str(v).replace(",", "."))
    except:
        return None

def merge_node(tx, label, keydict: dict, props: dict):
    keys = ", ".join([f"{k}: $key['{k}']" for k in keydict.keys()])
    q = f"MERGE (n:{label} {{{keys}}}) SET n += $props"
    tx.run(q, key=keydict, props=props)

def relate(tx, a_label, a_key, b_label, b_key, rel, rel_props=None):
    keys_a = " AND ".join([f"a.{k} = $a_key['{k}']" for k in a_key.keys()])
    keys_b = " AND ".join([f"b.{k} = $b_key['{k}']" for k in b_key.keys()])
    q = f"""
    MATCH (a:{a_label}), (b:{b_label})
    WHERE {keys_a} AND {keys_b}
    MERGE (a)-[r:{rel}]->(b)
    """
    if rel_props:
        q += " SET r += $rel_props"
    tx.run(q, a_key=a_key, b_key=b_key, rel_props=rel_props or {})

def main():
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {xlsx_path}")

    print(f"[info] Lendo planilha: {xlsx_path}")
    sheets = pd.read_excel(xlsx_path, sheet_name=None, dtype="object")
    df = sheets["ocorrencias"]
    print(f"[info] ocorrencias: {len(df)} linhas")

    driver = GraphDatabase.driver(URI, auth=(USER, PASS))
    with driver.session() as session:
        # --- Upsert dimensões (se existirem no arquivo) ---
        def upsert_dim(sheet, fn):
            if sheet in sheets and len(sheets[sheet]) > 0:
                fn(sheets[sheet].copy())
                print(f"[ok] {sheet}: {len(sheets[sheet])} linhas")

        def dim_municipio(tab):
            for _, r in tab.iterrows():
                if is_ok(r.get("CODIGO_MUNICIPIO")):
                    session.execute_write(
                        merge_node, "Municipio",
                        {"cod": r["CODIGO_MUNICIPIO"]},
                        {"cod": r["CODIGO_MUNICIPIO"], "nome": r.get("MUNICIPIO")}
                    )

        def dim_bairro(tab):
            for _, r in tab.iterrows():
                if is_ok(r.get("MUNICIPIO_COD")) and is_ok(r.get("BAIRRO")):
                    session.execute_write(
                        merge_node, "Bairro",
                        {"municipio_cod": r["MUNICIPIO_COD"], "nome": r["BAIRRO"]},
                        {"municipio_cod": r["MUNICIPIO_COD"], "nome": r["BAIRRO"]}
                    )
                    session.execute_write(
                        relate, "Bairro",
                        {"municipio_cod": r["MUNICIPIO_COD"], "nome": r["BAIRRO"]},
                        "Municipio", {"cod": r["MUNICIPIO_COD"]},
                        "FICA_EM"
                    )

        def dim_nat_p(tab):
            for _, r in tab.iterrows():
                if is_ok(r.get("CODIGO_NATUREZA_PRINCIPAL")):
                    session.execute_write(
                        merge_node, "NaturezaPrincipal",
                        {"codigo": r["CODIGO_NATUREZA_PRINCIPAL"]},
                        {"codigo": r["CODIGO_NATUREZA_PRINCIPAL"], "descricao": r.get("DESCR_NATUREZA_PRINCIPAL")}
                    )

        def dim_nat_s(tab):
            # pode ter colunas com nomes já normalizados
            col_cod = "CODIGO_NATUREZA_SECUNDARIA" if "CODIGO_NATUREZA_SECUNDARIA" in tab.columns else "codigo"
            col_desc = "DESCR_NATUREZA_SECUNDARIA" if "DESCR_NATUREZA_SECUNDARIA" in tab.columns else "descricao"
            for _, r in tab.iterrows():
                if is_ok(r.get(col_cod)):
                    session.execute_write(
                        merge_node, "NaturezaSecundaria",
                        {"codigo": r[col_cod]},
                        {"codigo": r[col_cod], "descricao": r.get(col_desc)}
                    )

        def dim_unidade(tab):
            for _, r in tab.iterrows():
                u5 = r.get("UNID_AREA_NIVEL_5")
                codigo = r.get("CODIGO_UNID_AREA_NIVEL_6") or r.get("codigo")
                nome = r.get("UNID_AREA_NIVEL_6") or r.get("nome")
                if is_ok(u5):
                    session.execute_write(merge_node, "UnidadeN5", {"nome": u5}, {"nome": u5})
                if is_ok(codigo):
                    session.execute_write(merge_node, "UnidadeN6", {"codigo": codigo}, {"codigo": codigo, "nome": nome})
                    if is_ok(u5):
                        session.execute_write(relate, "UnidadeN6", {"codigo": codigo}, "UnidadeN5", {"nome": u5}, "PERTENCE_A")

        def dim_setor(tab):
            for _, r in tab.iterrows():
                if is_ok(r.get("SETOR")):
                    session.execute_write(merge_node, "Setor", {"nome": r["SETOR"]}, {"nome": r["SETOR"]})

        def dim_subsetor(tab):
            for _, r in tab.iterrows():
                sub = r.get("SUB_SETOR")
                set_ = r.get("SETOR")
                if is_ok(sub):
                    session.execute_write(merge_node, "SubSetor", {"nome": sub}, {"nome": sub})
                    if is_ok(set_):
                        session.execute_write(relate, "SubSetor", {"nome": sub}, "Setor", {"nome": set_}, "PERTENCE_A")

        def dim_causa(tab):
            for _, r in tab.iterrows():
                if is_ok(r.get("CODIGO_CAUSA_PRESUMIDA")):
                    session.execute_write(
                        merge_node, "Causa",
                        {"codigo": r["CODIGO_CAUSA_PRESUMIDA"]},
                        {"codigo": r["CODIGO_CAUSA_PRESUMIDA"], "descricao": r.get("CAUSA_PRESUMIDA")}
                    )

        def dim_tempo(tab):
            # espera colunas: ANO, MES_NUMERICO, MES_DESCRICAO, DIA_DA_SEMANA_NUMERICO, DIA_DA_SEMANA_FATO, FAIXA_HORA_1, FAIXA_HORA_6
            for _, r in tab.iterrows():
                ano = to_int(r.get("ANO"))
                mes = to_int(r.get("MES_NUMERICO"))
                if ano is None or mes is None:
                    continue
                props = {
                    "ano": ano, "mes_num": mes,
                    "mes_desc": r.get("MES_DESCRICAO"),
                    "dia_semana_num": to_int(r.get("DIA_DA_SEMANA_NUMERICO")),
                    "dia_semana": r.get("DIA_DA_SEMANA_FATO"),
                    "faixa_h1": r.get("FAIXA_HORA_1"),
                    "faixa_h6": r.get("FAIXA_HORA_6"),
                }
                session.execute_write(merge_node, "Tempo", {"ano": ano, "mes_num": mes}, props)

        def dim_meio(tab):
            for _, r in tab.iterrows():
                if is_ok(r.get("DESCRICAO_MEIO_UTILIZADO")):
                    session.execute_write(
                        merge_node, "Meio",
                        {"descricao": r["DESCRICAO_MEIO_UTILIZADO"]},
                        {"descricao": r["DESCRICAO_MEIO_UTILIZADO"]}
                    )

        upsert_dim("dim_municipio",          dim_municipio)
        upsert_dim("dim_bairro",             dim_bairro)
        upsert_dim("dim_natureza_principal", dim_nat_p)
        upsert_dim("dim_natureza_secundaria",dim_nat_s)
        upsert_dim("dim_unidade",            dim_unidade)
        upsert_dim("dim_setor",              dim_setor)
        upsert_dim("dim_subsetor",           dim_subsetor)
        upsert_dim("dim_causa",              dim_causa)
        upsert_dim("dim_tempo",              dim_tempo)
        upsert_dim("dim_meio",               dim_meio)

        # --- Ocorrencias + relações ---
        total = len(df)
        for i in range(0, total, BATCH):
            chunk = df.iloc[i:i+BATCH]
            print(f"[info] carregando ocorrencias {i+1}-{i+len(chunk)} / {total}")
            for _, r in chunk.iterrows():
                oc_key = {"NUMERO_REDS": str(r.get("NUMERO_REDS"))}
                if not is_ok(oc_key["NUMERO_REDS"]):
                    continue

                oc_props = {
                    "NUMERO_REDS": oc_key["NUMERO_REDS"],
                    "data": str(r.get("DATA_FATO")),
                    "hora": str(r.get("HORARIO_FATO")),
                    "lat": to_float(r.get("LATITUDE")),
                    "lon": to_float(r.get("LONGITUDE")),
                    "prisao": to_int(r.get("QTDE_PRISAO")),
                    "imv": to_int(r.get("IMV_TOTAL")),
                    "icvpe": to_int(r.get("ICVPE_TOTAL")),
                    "icvpa": to_int(r.get("ICVPA_TOTAL")),
                }
                session.execute_write(merge_node, "Ocorrencia", oc_key, oc_props)

                # Bairro / Município
                if is_ok(r.get("BAIRRO")) and is_ok(r.get("CODIGO_MUNICIPIO")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                         "Bairro", {"municipio_cod": r["CODIGO_MUNICIPIO"], "nome": r["BAIRRO"]},
                                         "OCORRE_EM")

                # Unidades
                if is_ok(r.get("UNID_AREA_NIVEL_5")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "UnidadeN5", {"nome": r["UNID_AREA_NIVEL_5"]},
                                          "AREA_N5")
                if is_ok(r.get("CODIGO_UNID_AREA_NIVEL_6")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "UnidadeN6", {"codigo": r["CODIGO_UNID_AREA_NIVEL_6"]},
                                          "AREA_N6")

                # Setor / Subsetor
                if is_ok(r.get("SETOR")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "Setor", {"nome": r["SETOR"]},
                                          "SETOR")
                if is_ok(r.get("SUB_SETOR")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "SubSetor", {"nome": r["SUB_SETOR"]},
                                          "SUBSETOR")

                # Naturezas
                if is_ok(r.get("CODIGO_NATUREZA_PRINCIPAL")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "NaturezaPrincipal", {"codigo": r["CODIGO_NATUREZA_PRINCIPAL"]},
                                          "CLASSIFICADA_COM",
                                          {"tentcons": r.get("TENTADO_CONSUMADO_PRINCIPAL")})
                if is_ok(r.get("CODIGO_NATUREZA_SECUNDARIA1")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "NaturezaSecundaria", {"codigo": r["CODIGO_NATUREZA_SECUNDARIA1"]},
                                          "RELACIONA_SE",
                                          {"tentcons": r.get("TENTADO_CONSUMADO_SECUNDARIA1")})
                if is_ok(r.get("CODIGO_NATUREZA_SECUNDARIA2")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "NaturezaSecundaria", {"codigo": r["CODIGO_NATUREZA_SECUNDARIA2"]},
                                          "RELACIONA_SE",
                                          {"tentcons": r.get("TENTADO_CONSUMADO_SECUNDARIA2")})

                # Tempo
                if is_ok(r.get("ANO_FATO")) and is_ok(r.get("MES_NUMERICO")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "Tempo", {"ano": to_int(r["ANO_FATO"]), "mes_num": to_int(r["MES_NUMERICO"])},
                                          "NO_TEMPO")

                # Causa e Meio
                if is_ok(r.get("CODIGO_CAUSA_PRESUMIDA")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "Causa", {"codigo": r["CODIGO_CAUSA_PRESUMIDA"]},
                                          "CAUSA")
                if is_ok(r.get("DESCRICAO_MEIO_UTILIZADO")):
                    session.execute_write(relate, "Ocorrencia", oc_key,
                                          "Meio", {"descricao": r["DESCRICAO_MEIO_UTILIZADO"]},
                                          "MEIO")
    driver.close()
    print("✔ Carga concluída.")

if __name__ == "__main__":
    main()
