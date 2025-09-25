import pandas as pd
from pathlib import Path

ARQ = Path(r"E:\TCC\Interface\Projeto_Grafo_TCC\data\modelo_grafo_REDS_v2.xlsx")

def clean_series(s: pd.Series) -> pd.Series:
    if s.dtype == "O":
        # strip, normaliza espaços; mantém acentuação
        return s.astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA})
    return s

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        df[c] = clean_series(df[c])
    return df

def main():
    # 1) lê apenas a aba ocorrencias
    df = pd.read_excel(ARQ, sheet_name="ocorrencias", dtype="object")
    df = clean_df(df)

    # 2) monta cada dimensão com base nas colunas da ocorrencias
    # MUNICÍPIO
    dim_municipio = (
        df[["CODIGO_MUNICIPIO", "MUNICIPIO"]]
        .dropna(how="any")
        .drop_duplicates()
        .sort_values(["MUNICIPIO", "CODIGO_MUNICIPIO"], na_position="last")
    )

    # BAIRRO (chave composta: municipio + bairro)
    dim_bairro = (
        df[["CODIGO_MUNICIPIO", "BAIRRO"]]
        .rename(columns={"CODIGO_MUNICIPIO": "MUNICIPIO_COD"})
        .dropna(how="any")
        .drop_duplicates()
        .sort_values(["MUNICIPIO_COD", "BAIRRO"], na_position="last")
    )

    # NATUREZA PRINCIPAL
    dim_nat_p = (
        df[["CODIGO_NATUREZA_PRINCIPAL", "DESCR_NATUREZA_PRINCIPAL"]]
        .dropna(how="any")
        .drop_duplicates()
        .sort_values(["DESCR_NATUREZA_PRINCIPAL", "CODIGO_NATUREZA_PRINCIPAL"], na_position="last")
    )

    # NATUREZA SECUNDÁRIA (une 1 e 2)
    n1 = df[["CODIGO_NATUREZA_SECUNDARIA1", "DESCR_NATUREZA_SECUNDARIA1"]].dropna(how="any")
    n1.columns = ["CODIGO_NATUREZA_SECUNDARIA", "DESCR_NATUREZA_SECUNDARIA"]
    n2 = df[["CODIGO_NATUREZA_SECUNDARIA2", "DESCR_NATUREZA_SECUNDARIA2"]].dropna(how="any")
    n2.columns = ["CODIGO_NATUREZA_SECUNDARIA", "DESCR_NATUREZA_SECUNDARIA"]
    dim_nat_s = (
        pd.concat([n1, n2], ignore_index=True)
        .drop_duplicates()
        .sort_values(["DESCR_NATUREZA_SECUNDARIA", "CODIGO_NATUREZA_SECUNDARIA"], na_position="last")
    )

    # UNIDADE (N5/N6)
    dim_unidade = (
        df[["UNID_AREA_NIVEL_5", "CODIGO_UNID_AREA_NIVEL_6", "UNID_AREA_NIVEL_6"]]
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["UNID_AREA_NIVEL_5", "UNID_AREA_NIVEL_6", "CODIGO_UNID_AREA_NIVEL_6"], na_position="last")
    )

    # SETOR
    dim_setor = (
        df[["SETOR"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["SETOR"], na_position="last")
    )

    # SUBSETOR (com SETOR para relação)
    dim_subsetor = (
        df[["SUB_SETOR", "SETOR"]]
        .dropna(subset=["SUB_SETOR"])
        .drop_duplicates()
        .sort_values(["SETOR", "SUB_SETOR"], na_position="last")
    )

    # CAUSA PRESUMIDA
    dim_causa = (
        df[["CODIGO_CAUSA_PRESUMIDA", "CAUSA_PRESUMIDA"]]
        .dropna(how="any")
        .drop_duplicates()
        .sort_values(["CAUSA_PRESUMIDA", "CODIGO_CAUSA_PRESUMIDA"], na_position="last")
    )

    # TEMPO
    dim_tempo = (
        df[
            [
                "ANO_FATO",
                "MES_NUMERICO",
                "MES_DESCRICAO",
                "DIA_DA_SEMANA_NUMERICO",
                "DIA_DA_SEMANA_FATO",
                "FAIXA_HORA_1",
                "FAIXA_HORA_6",
            ]
        ]
        .rename(columns={"ANO_FATO": "ANO"})
        .dropna(how="all")
        .drop_duplicates()
        .sort_values(["ANO", "MES_NUMERICO"], na_position="last")
    )

    # MEIO
    dim_meio = (
        df[["DESCRICAO_MEIO_UTILIZADO"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["DESCRICAO_MEIO_UTILIZADO"], na_position="last")
    )

    # 3) escreve de volta — preservando a aba ocorrencias
    #    usamos openpyxl para ler e regravar as sheets de dimensão
    with pd.ExcelWriter(ARQ, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
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

    print("✔ Dimensões atualizadas com sucesso.")

if __name__ == "__main__":
    if not ARQ.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ARQ}")
    main()
