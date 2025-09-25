import pandas as pd
from pathlib import Path
from datetime import datetime

ARQ = Path(r"E:\TCC\Interface\Projeto_Grafo_TCC\data\modelo_grafo_REDS_v2.xlsx")

def clean_series(s: pd.Series) -> pd.Series:
    s = s.astype("string")  # dtype string (pandas) ajuda nas limpezas
    # remove NBSP e normaliza espaços
    s = s.str.replace("\u00A0", " ", regex=False)
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    # trata vazios comuns
    return s.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA})

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        out[c] = clean_series(out[c])
    return out

def preview_count(name, df):
    print(f"[preview] {name}: {len(df)} linhas")

def main():
    if not ARQ.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ARQ}")

    print(f"--> Lendo: {ARQ}")
    df = pd.read_excel(ARQ, sheet_name="ocorrencias", dtype="object")
    print(f"[info] ocorrencias lidas: {len(df)}")
    if len(df) == 0:
        raise RuntimeError("A aba 'ocorrencias' está vazia (ou não foi lida).")

    df = clean_df(df)

    # --------- Dimensões ---------
    # MUNICÍPIO
    dim_municipio = (
        df[["CODIGO_MUNICIPIO", "MUNICIPIO"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["MUNICIPIO", "CODIGO_MUNICIPIO"], na_position="last")
    )
    preview_count("dim_municipio", dim_municipio)

    # BAIRRO (chave composta: municipio + bairro)
    dim_bairro = (
        df[["CODIGO_MUNICIPIO", "BAIRRO"]]
        .rename(columns={"CODIGO_MUNICIPIO": "MUNICIPIO_COD"})
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["MUNICIPIO_COD", "BAIRRO"], na_position="last")
    )
    preview_count("dim_bairro", dim_bairro)

    # NATUREZA PRINCIPAL
    dim_nat_p = (
        df[["CODIGO_NATUREZA_PRINCIPAL", "DESCR_NATUREZA_PRINCIPAL"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["DESCR_NATUREZA_PRINCIPAL", "CODIGO_NATUREZA_PRINCIPAL"], na_position="last")
    )
    preview_count("dim_natureza_principal", dim_nat_p)

    # NATUREZA SECUNDÁRIA (une 1 e 2)
    n1 = df[["CODIGO_NATUREZA_SECUNDARIA1", "DESCR_NATUREZA_SECUNDARIA1"]].dropna(how="all")
    n1.columns = ["CODIGO_NATUREZA_SECUNDARIA", "DESCR_NATUREZA_SECUNDARIA"]
    n2 = df[["CODIGO_NATUREZA_SECUNDARIA2", "DESCR_NATUREZA_SECUNDARIA2"]].dropna(how="all")
    n2.columns = ["CODIGO_NATUREZA_SECUNDARIA", "DESCR_NATUREZA_SECUNDARIA"]
    dim_nat_s = (
        pd.concat([n1, n2], ignore_index=True)
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["DESCR_NATUREZA_SECUNDARIA", "CODIGO_NATUREZA_SECUNDARIA"], na_position="last")
    )
    preview_count("dim_natureza_secundaria", dim_nat_s)

    # UNIDADE (N5/N6)
    dim_unidade = (
        df[["UNID_AREA_NIVEL_5", "CODIGO_UNID_AREA_NIVEL_6", "UNID_AREA_NIVEL_6"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["UNID_AREA_NIVEL_5", "UNID_AREA_NIVEL_6", "CODIGO_UNID_AREA_NIVEL_6"], na_position="last")
    )
    preview_count("dim_unidade", dim_unidade)

    # SETOR
    dim_setor = df[["SETOR"]].dropna(how="all").drop_duplicates().sort_values(["SETOR"], na_position="last")
    preview_count("dim_setor", dim_setor)

    # SUBSETOR
    dim_subsetor = (
        df[["SUB_SETOR", "SETOR"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["SETOR", "SUB_SETOR"], na_position="last")
    )
    preview_count("dim_subsetor", dim_subsetor)

    # CAUSA PRESUMIDA
    dim_causa = (
        df[["CODIGO_CAUSA_PRESUMIDA", "CAUSA_PRESUMIDA"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["CAUSA_PRESUMIDA", "CODIGO_CAUSA_PRESUMIDA"], na_position="last")
    )
    preview_count("dim_causa", dim_causa)

    # TEMPO
    dim_tempo = (
        df[[
            "ANO_FATO","MES_NUMERICO","MES_DESCRICAO",
            "DIA_DA_SEMANA_NUMERICO","DIA_DA_SEMANA_FATO",
            "FAIXA_HORA_1","FAIXA_HORA_6",
        ]]
        .rename(columns={"ANO_FATO": "ANO"})
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["ANO", "MES_NUMERICO"], na_position="last")
    )
    preview_count("dim_tempo", dim_tempo)

    # MEIO
    dim_meio = (
        df[["DESCRICAO_MEIO_UTILIZADO"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["DESCRICAO_MEIO_UTILIZADO"], na_position="last")
    )
    preview_count("dim_meio", dim_meio)

    # --------- Backup + Escrita ---------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = ARQ.with_name(f"{ARQ.stem}_backup_{ts}{ARQ.suffix}")
    ARQ.replace(backup)  # move o original para backup
    print(f"[info] backup criado: {backup}")

    # regrava tudo: ocorrencias (a partir do backup) + dimensões
    base = pd.read_excel(backup, sheet_name=None)  # lê TODAS as abas do backup
    with pd.ExcelWriter(ARQ, engine="openpyxl", mode="w") as writer:
        # escreve de volta a ocorrencias do backup
        base["ocorrencias"].to_excel(writer, index=False, sheet_name="ocorrencias")
        # escreve as dimensões
        dim_municipio.to_excel(writer, index=False, sheet_name="dim_municipio")
        dim_bairro.to_excel(writer, index=False, sheet_name="dim_bairro")
        dim_nat_p.to_excel(writer, index=False, sheet_name="dim_natureza_principal")
        dim_nat_s.to_excel(writer, index=False, sheet_name="dim_natureza_secundaria")
        dim_unidade.to_excel(writer, index=False, sheet_name="dim_unidade")
        dim_setor.to_excel(writer, index=False, sheet_name="dim_setor")
        dim_subsetor.to_excel(writer, index=False, sheet_name="dim_subsetor")
        dim_causa.to_excel(writer, index=False, sheet_name="dim_causa")
        dim_tempo.to_excel(writer, index=False, sheet_name="dim_tempo")
        dim_meio.to_excel(writer, index=False, sheet_name="dim_meio")

    # --------- Validação pós-escrita ---------
    check = pd.read_excel(ARQ, sheet_name=None)
    for k in ["dim_municipio","dim_bairro","dim_natureza_principal","dim_natureza_secundaria",
              "dim_unidade","dim_setor","dim_subsetor","dim_causa","dim_tempo","dim_meio"]:
        print(f"[check] {k}: {len(check[k])} linhas")

    print("✔ Dimensões atualizadas (e verificadas) com sucesso.")

if __name__ == "__main__":
    main()
