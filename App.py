import calendar
import os
import zipfile
from datetime import date, datetime

from logger import log_time

import altair as alt
import pandas as pd
import streamlit as st

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode, JsCode
    try:
        _AGGRID_VERSION = getattr(__import__("st_aggrid"), "__version__", None)
    except Exception:
        _AGGRID_VERSION = None

    _AGGRID_AVAILABLE = True
except Exception:
    AgGrid = None
    GridOptionsBuilder = None
    DataReturnMode = None
    GridUpdateMode = None
    JsCode = None
    _AGGRID_AVAILABLE = False
    _AGGRID_VERSION = None


DATA_FILE = "presupuesto.xlsx"
CATEGORIAS = [
    "Hogar",
    "Telefonia",
    "Franco",
    "Automovil",
    "Educacion",
    "Suscripcion",
    "Deuda",
    "Ocio",
]


def _write_empty_data_file(path: str) -> None:
    gastos_mensuales = _empty_gastos_mensuales_df()
    ingresos_mensuales = _empty_ingresos_mensuales_df()
    comentarios = _empty_comentarios_df()
    gastos = pd.DataFrame(
        columns=[
            "gasto_id",
            "nombre",
            "categoria",
            "monto_presupuestado",
            "periodicidad",
            "fecha_pago",
            "fecha_inicio",
            "fecha_termino",
        ]
    )
    pagos = pd.DataFrame(
        columns=[
            "pago_id",
            "gasto_id",
            "monto_real",
            "fecha_pago_real",
            "estado",
        ]
    )
    ingresos = pd.DataFrame(
        columns=[
            "ingreso_id",
            "nombre",
            "monto",
            "periodicidad",
            "fecha_pago",
            "fecha_inicio",
            "fecha_termino",
        ]
    )
    cuenta = pd.DataFrame(columns=["saldo_actual"])
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        gastos.to_excel(writer, sheet_name="gastos", index=False)
        pagos.to_excel(writer, sheet_name="pagos", index=False)
        ingresos.to_excel(writer, sheet_name="ingresos", index=False)
        cuenta.to_excel(writer, sheet_name="cuenta", index=False)
        gastos_mensuales.to_excel(writer, sheet_name="gastos_mensuales", index=False)
        ingresos_mensuales.to_excel(writer, sheet_name="ingresos_mensuales", index=False)
        comentarios.to_excel(writer, sheet_name="comentarios", index=False)


def _empty_gastos_mensuales_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "gasto_id",
            "year",
            "month",
            "monto_presupuestado",
        ]
    )


def _empty_ingresos_mensuales_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ingreso_id",
            "year",
            "month",
            "monto",
        ]
    )


def _empty_comentarios_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["comentario"])


@log_time
def _read_data_file(path: str) -> dict:
    data = {
        "gastos": pd.read_excel(path, sheet_name="gastos"),
        "pagos": pd.read_excel(path, sheet_name="pagos"),
        "ingresos": pd.read_excel(path, sheet_name="ingresos"),
        "cuenta": pd.read_excel(path, sheet_name="cuenta"),
    }
    try:
        gastos_mensuales = pd.read_excel(path, sheet_name="gastos_mensuales")
    except ValueError:
        gastos_mensuales = _empty_gastos_mensuales_df()
    try:
        legacy_ajustes = pd.read_excel(path, sheet_name="ajustes")
    except ValueError:
        legacy_ajustes = _empty_gastos_mensuales_df()

    if not legacy_ajustes.empty:
        merged = gastos_mensuales.copy()
        if merged.empty:
            merged = _empty_gastos_mensuales_df()
        existing_keys = set(
            zip(
                merged["gasto_id"].astype(int),
                merged["year"].astype(int),
                merged["month"].astype(int),
            )
        )
        to_add = []
        for _, row in legacy_ajustes.iterrows():
            key = (int(row["gasto_id"]), int(row["year"]), int(row["month"]))
            if key in existing_keys:
                continue
            to_add.append(
                {
                    "gasto_id": int(row["gasto_id"]),
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "monto_presupuestado": float(row["monto_presupuestado"])
                    if not pd.isna(row["monto_presupuestado"])
                    else 0.0,
                }
            )
        if to_add:
            gastos_mensuales = pd.concat([merged, pd.DataFrame(to_add)], ignore_index=True)
        else:
            gastos_mensuales = merged

    data["gastos_mensuales"] = gastos_mensuales
    try:
        ingresos_mensuales = pd.read_excel(path, sheet_name="ingresos_mensuales")
    except ValueError:
        ingresos_mensuales = _empty_ingresos_mensuales_df()
    data["ingresos_mensuales"] = ingresos_mensuales
    try:
        comentarios = pd.read_excel(path, sheet_name="comentarios")
    except ValueError:
        comentarios = _empty_comentarios_df()
    data["comentarios"] = comentarios
    return data


def _ensure_data_file(path: str) -> None:
    if os.path.exists(path):
        return
    _write_empty_data_file(path)


@st.cache_data
@log_time
def _load_data(path: str) -> dict:
    _ensure_data_file(path)
    try:
        return _read_data_file(path)
    except zipfile.BadZipFile:
        backup_path = f"{path}.bak"
        if os.path.exists(path) and not os.path.exists(backup_path):
            try:
                os.replace(path, backup_path)
            except PermissionError:
                st.error(
                    "El archivo presupuesto.xlsx esta abierto o bloqueado. "
                    "Cierralo y vuelve a intentar."
                )
                st.stop()
        try:
            _write_empty_data_file(path)
            return _read_data_file(path)
        except Exception:
            st.error(
                "No se pudo recrear presupuesto.xlsx. Cierralo si esta abierto "
                "y elimina el archivo para regenerarlo."
            )
            st.stop()


@log_time
def _save_data(path: str, data: dict) -> None:
    temp_path = f"{path}.tmp.xlsx"
    gastos_mensuales_df = data.get("gastos_mensuales", _empty_gastos_mensuales_df())
    ingresos_mensuales_df = data.get("ingresos_mensuales", _empty_ingresos_mensuales_df())
    comentarios_df = data.get("comentarios", _empty_comentarios_df())
    with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
        data["gastos"].to_excel(writer, sheet_name="gastos", index=False)
        data["pagos"].to_excel(writer, sheet_name="pagos", index=False)
        data["ingresos"].to_excel(writer, sheet_name="ingresos", index=False)
        data["cuenta"].to_excel(writer, sheet_name="cuenta", index=False)
        gastos_mensuales_df.to_excel(writer, sheet_name="gastos_mensuales", index=False)
        ingresos_mensuales_df.to_excel(writer, sheet_name="ingresos_mensuales", index=False)
        comentarios_df.to_excel(writer, sheet_name="comentarios", index=False)
    os.replace(temp_path, path)


def _next_id(series: pd.Series) -> int:
    if series.empty:
        return 1
    return int(series.max()) + 1


def _parse_date(value) -> date | None:
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def _is_due_in_month(periodicidad: str, fecha_pago, year: int, month: int) -> bool:
    if periodicidad == "Mensual":
        return True
    fecha = _parse_date(fecha_pago)
    if not fecha:
        return False
    return fecha.month == month


def _is_active_for_month(fecha_inicio, fecha_termino, year: int, month: int) -> bool:
    inicio = _parse_date(fecha_inicio) or date(year, month, 1)
    termino = _parse_date(fecha_termino) or date(year, month, 28)
    period_start = date(year, month, 1)
    period_end = date(year, month, 28)
    return inicio <= period_end and termino >= period_start


def _current_month() -> tuple[int, int]:
    today = date.today()
    return today.year, today.month


def _month_label(month: int) -> str:
    labels = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]
    return labels[month - 1]


def _month_options() -> list[tuple[str, int]]:
    return [(_month_label(month), month) for month in range(1, 13)]


def _month_range(start: date, end: date) -> list[tuple[int, int]]:
    if not start or not end:
        return []
    if end < start:
        start, end = end, start
    current = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    months = []
    while current <= end_month:
        months.append((current.year, current.month))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _months_for_row(periodicidad: str, fecha_pago, fecha_inicio, fecha_termino) -> list[tuple[int, int]]:
    inicio = _parse_date(fecha_inicio)
    termino = _parse_date(fecha_termino)
    months = _month_range(inicio, termino)
    if periodicidad == "Anual":
        pago = _parse_date(fecha_pago)
        if pago:
            months = [month for month in months if month[1] == pago.month]
        else:
            months = []
    return months


def _monthly_day_value(value, fallback: int = 1) -> int:
    if pd.isna(value):
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


def _normalize_text(value: str) -> str:
    return value.strip()


def _format_amount(value) -> str:
    if pd.isna(value):
        return "0"
    try:
        numeric = float(value)
    except Exception:
        return "0"
    return f"{numeric:,.0f}".replace(",", ".")


def _format_amount_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    display_df = df.copy()
    for col in columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(_format_amount)
    return display_df


def _render_aggrid_sum_view(df: pd.DataFrame, key: str) -> None:
    if df is None or df.empty:
        st.info("No hay datos para mostrar.")
        return
    if not _AGGRID_AVAILABLE:
        st.info("AgGrid no esta instalado. Agrega streamlit-aggrid en requirements.txt.")
        return
    builder = GridOptionsBuilder.from_dataframe(df)
    builder.configure_default_column(editable=False, enableValue=True)
    number_formatter = None
    if JsCode is not None:
        number_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              var v = Number(params.value);
              if (isNaN(v)) { return params.value; }
              return v.toLocaleString('es-CL');
            }
            """
        )

    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        col_name = str(col).lower()
        if col_name.endswith("_id") or col_name == "id":
            continue
        if number_formatter is not None:
            builder.configure_column(col, valueFormatter=number_formatter, aggFunc="sum")
        else:
            builder.configure_column(col, aggFunc="sum")

    builder.configure_grid_options(
        enableRangeSelection=True,
        statusBar={
            "statusPanels": [
                {
                    "statusPanel": "agAggregationComponent",
                    "statusPanelParams": {"aggFuncs": ["sum"]},
                }
            ]
        },
    )

    AgGrid(
        df,
        gridOptions=builder.build(),
        enable_enterprise_modules=True,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.NO_UPDATE,
        height=320,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        key=key,
    )


def _sort_by_categoria_nombre(
    df: pd.DataFrame,
    categoria_col: str = "categoria",
    nombre_col: str = "nombre",
) -> pd.DataFrame:
    if df.empty or categoria_col not in df.columns or nombre_col not in df.columns:
        return df
    return df.sort_values([categoria_col, nombre_col], kind="stable").reset_index(drop=True)


def _parse_amount(value) -> float:
    if pd.isna(value):
        return 0.0
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "":
            return 0.0
        cleaned = cleaned.replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except Exception:
            return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _load_or_init_saldo(cuenta_df: pd.DataFrame) -> float:
    if cuenta_df.empty:
        return 0.0
    value = cuenta_df.iloc[0]["saldo_actual"]
    if pd.isna(value):
        return 0.0
    return float(value)


def _set_saldo(cuenta_df: pd.DataFrame, saldo: float) -> pd.DataFrame:
    return pd.DataFrame([{"saldo_actual": saldo}])


def _paid_in_month(pagos_df: pd.DataFrame, gasto_id: int, year: int, month: int) -> bool:
    pagos_gasto = pagos_df[pagos_df["gasto_id"] == gasto_id]
    if pagos_gasto.empty:
        return False
    fechas = pagos_gasto["fecha_pago_real"].apply(_parse_date)
    for fecha in fechas:
        if not fecha:
            continue
        if fecha.year == year and fecha.month == month:
            return True
    return False


def _gastos_mensuales_map_for_year(
    gastos_mensuales_df: pd.DataFrame, year: int
) -> dict[tuple[int, int], float]:
    if gastos_mensuales_df is None or gastos_mensuales_df.empty:
        return {}
    year_df = gastos_mensuales_df.copy()
    year_df = year_df[year_df["year"].astype(int) == int(year)]
    ajustes = {}
    for _, row in year_df.iterrows():
        monto = row.get("monto_presupuestado")
        ajustes[(int(row["gasto_id"]), int(row["month"]))] = (
            float(monto) if not pd.isna(monto) else 0.0
        )
    return ajustes


def _ingresos_mensuales_map_for_year(
    ingresos_mensuales_df: pd.DataFrame, year: int
) -> dict[tuple[int, int], float]:
    if ingresos_mensuales_df is None or ingresos_mensuales_df.empty:
        return {}
    year_df = ingresos_mensuales_df.copy()
    year_df = year_df[year_df["year"].astype(int) == int(year)]
    ingresos = {}
    for _, row in year_df.iterrows():
        monto = row.get("monto")
        ingresos[(int(row["ingreso_id"]), int(row["month"]))] = (
            float(monto) if not pd.isna(monto) else 0.0
        )
    return ingresos


def _presupuesto_for_month(row: pd.Series, year: int, month: int, gastos_mensuales_map: dict) -> float:
    override = gastos_mensuales_map.get((int(row["gasto_id"]), int(month)))
    if override is not None:
        return float(override)
    return 0.0


def _monto_ingreso_for_month(
    row: pd.Series,
    year: int,
    month: int,
    ingresos_mensuales_map: dict,
) -> float:
    override = ingresos_mensuales_map.get((int(row["ingreso_id"]), int(month)))
    if override is not None:
        return float(override)
    return 0.0


def _append_gasto_mensual_entries(
    gastos_mensuales_df: pd.DataFrame,
    gasto_row: dict,
) -> pd.DataFrame:
    periodicidad = str(gasto_row.get("periodicidad", ""))
    months = _months_for_row(
        periodicidad,
        gasto_row.get("fecha_pago"),
        gasto_row.get("fecha_inicio"),
        gasto_row.get("fecha_termino"),
    )
    if not months:
        return gastos_mensuales_df
    monto = float(gasto_row.get("monto_presupuestado", 0.0))
    new_rows = [
        {
            "gasto_id": int(gasto_row["gasto_id"]),
            "year": int(year),
            "month": int(month),
            "monto_presupuestado": monto,
        }
        for year, month in months
    ]
    if gastos_mensuales_df is None or gastos_mensuales_df.empty:
        return pd.DataFrame(new_rows)
    return pd.concat([gastos_mensuales_df, pd.DataFrame(new_rows)], ignore_index=True)


def _append_ingreso_mensual_entries(
    ingresos_mensuales_df: pd.DataFrame,
    ingreso_row: dict,
) -> pd.DataFrame:
    periodicidad = str(ingreso_row.get("periodicidad", ""))
    months = _months_for_row(
        periodicidad,
        ingreso_row.get("fecha_pago"),
        ingreso_row.get("fecha_inicio"),
        ingreso_row.get("fecha_termino"),
    )
    if not months:
        return ingresos_mensuales_df
    monto = float(ingreso_row.get("monto", 0.0))
    new_rows = [
        {
            "ingreso_id": int(ingreso_row["ingreso_id"]),
            "year": int(year),
            "month": int(month),
            "monto": monto,
        }
        for year, month in months
    ]
    if ingresos_mensuales_df is None or ingresos_mensuales_df.empty:
        return pd.DataFrame(new_rows)
    return pd.concat([ingresos_mensuales_df, pd.DataFrame(new_rows)], ignore_index=True)


def _migrate_mensuales_from_base(
    gastos_df: pd.DataFrame,
    ingresos_df: pd.DataFrame,
    gastos_mensuales_df: pd.DataFrame,
    ingresos_mensuales_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    changed = False

    if gastos_mensuales_df is None or gastos_mensuales_df.empty:
        gastos_mensuales_df = _empty_gastos_mensuales_df()
    existing_gastos = set(
        zip(
            gastos_mensuales_df["gasto_id"].astype(int),
            gastos_mensuales_df["year"].astype(int),
            gastos_mensuales_df["month"].astype(int),
        )
    )
    new_gastos_rows = []
    for _, row in gastos_df.iterrows():
        months = _months_for_row(
            str(row.get("periodicidad", "")),
            row.get("fecha_pago"),
            row.get("fecha_inicio"),
            row.get("fecha_termino"),
        )
        for year, month in months:
            key = (int(row["gasto_id"]), int(year), int(month))
            if key in existing_gastos:
                continue
            new_gastos_rows.append(
                {
                    "gasto_id": int(row["gasto_id"]),
                    "year": int(year),
                    "month": int(month),
                    "monto_presupuestado": float(row.get("monto_presupuestado", 0.0)),
                }
            )
            existing_gastos.add(key)
    if new_gastos_rows:
        gastos_mensuales_df = pd.concat(
            [gastos_mensuales_df, pd.DataFrame(new_gastos_rows)], ignore_index=True
        )
        changed = True

    if ingresos_mensuales_df is None or ingresos_mensuales_df.empty:
        ingresos_mensuales_df = _empty_ingresos_mensuales_df()
    existing_ingresos = set(
        zip(
            ingresos_mensuales_df["ingreso_id"].astype(int),
            ingresos_mensuales_df["year"].astype(int),
            ingresos_mensuales_df["month"].astype(int),
        )
    )
    new_ingresos_rows = []
    for _, row in ingresos_df.iterrows():
        months = _months_for_row(
            str(row.get("periodicidad", "")),
            row.get("fecha_pago"),
            row.get("fecha_inicio"),
            row.get("fecha_termino"),
        )
        for year, month in months:
            key = (int(row["ingreso_id"]), int(year), int(month))
            if key in existing_ingresos:
                continue
            new_ingresos_rows.append(
                {
                    "ingreso_id": int(row["ingreso_id"]),
                    "year": int(year),
                    "month": int(month),
                    "monto": float(row.get("monto", 0.0)),
                }
            )
            existing_ingresos.add(key)
    if new_ingresos_rows:
        ingresos_mensuales_df = pd.concat(
            [ingresos_mensuales_df, pd.DataFrame(new_ingresos_rows)], ignore_index=True
        )
        changed = True

    return gastos_mensuales_df, ingresos_mensuales_df, changed


def _get_pago_for_month(
    pagos_df: pd.DataFrame, gasto_id: int, year: int, month: int
) -> pd.Series | None:
    if pagos_df.empty:
        return None
    pagos_gasto = pagos_df[pagos_df["gasto_id"] == gasto_id]
    if pagos_gasto.empty:
        return None
    fechas = pagos_gasto["fecha_pago_real"].apply(_parse_date)
    for idx, fecha in fechas.items():
        if not fecha:
            continue
        if fecha.year == year and fecha.month == month:
            return pagos_gasto.loc[idx]
    return None


def _gastos_for_editor(gastos_df: pd.DataFrame) -> pd.DataFrame:
    editor_df = _sort_by_categoria_nombre(gastos_df.copy())
    for col in ["fecha_inicio", "fecha_termino"]:
        editor_df[col] = pd.to_datetime(editor_df[col], errors="coerce")
    editor_df["fecha_pago"] = editor_df["fecha_pago"].apply(
        lambda value: "" if pd.isna(value) else str(value)
    )
    return editor_df


def _ingresos_for_editor(ingresos_df: pd.DataFrame) -> pd.DataFrame:
    editor_df = ingresos_df.copy()
    for col in ["fecha_inicio", "fecha_termino"]:
        editor_df[col] = pd.to_datetime(editor_df[col], errors="coerce")
    editor_df["fecha_pago"] = editor_df["fecha_pago"].apply(
        lambda value: "" if pd.isna(value) else str(value)
    )
    return editor_df


def _apply_editor_gastos(editor_df: pd.DataFrame) -> pd.DataFrame:
    df = editor_df.copy()
    df["nombre"] = df["nombre"].astype(str).map(_normalize_text)
    df["monto_presupuestado"] = df["monto_presupuestado"].apply(_parse_amount)
    for col in ["fecha_inicio", "fecha_termino"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    def _parse_editor_fecha_pago(row: pd.Series):
        value = row.get("fecha_pago")
        if pd.isna(value) or str(value).strip() == "":
            return None
        if row.get("periodicidad") == "Mensual":
            try:
                return int(float(value))
            except Exception:
                return None
        parsed = pd.to_datetime(value, errors="coerce")
        return parsed.date() if not pd.isna(parsed) else None

    df["fecha_pago"] = df.apply(_parse_editor_fecha_pago, axis=1)
    return df


def _apply_editor_ingresos(editor_df: pd.DataFrame) -> pd.DataFrame:
    df = editor_df.copy()
    df["nombre"] = df["nombre"].astype(str).map(_normalize_text)
    df["monto"] = df["monto"].apply(_parse_amount)
    for col in ["fecha_inicio", "fecha_termino"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    def _parse_editor_fecha_pago(row: pd.Series):
        value = row.get("fecha_pago")
        if pd.isna(value) or str(value).strip() == "":
            return None
        if row.get("periodicidad") == "Mensual":
            try:
                return int(float(value))
            except Exception:
                return None
        parsed = pd.to_datetime(value, errors="coerce")
        return parsed.date() if not pd.isna(parsed) else None

    df["fecha_pago"] = df.apply(_parse_editor_fecha_pago, axis=1)
    return df


@st.cache_data
def _build_gastos_por_mes_table(
    gastos_df: pd.DataFrame,
    year: int,
    gastos_mensuales_df: pd.DataFrame | None = None,
    include_gasto_id: bool = False,
) -> pd.DataFrame:
    
    if gastos_df.empty:
        return pd.DataFrame()
    month_labels = [_month_label(month) for month in range(1, 13)]
    gastos_mensuales_map = (
        _gastos_mensuales_map_for_year(gastos_mensuales_df, year)
        if gastos_mensuales_df is not None
        else {}
    )
    rows = []
    for _, row in gastos_df.iterrows():
        row_data = {
            "gasto_id": int(row["gasto_id"]),
            "Gasto": str(row["nombre"]),
            "Categoria": str(row["categoria"]),
        }
        for month in range(1, 13):
            row_data[_month_label(month)] = _presupuesto_for_month(
                row,
                year,
                month,
                gastos_mensuales_map,
            )
        rows.append(row_data)
    table_df = pd.DataFrame(rows)
    if "Categoria" in table_df.columns and "Gasto" in table_df.columns:
        table_df = table_df.sort_values(["Categoria", "Gasto"], kind="stable")
    if include_gasto_id:
        return table_df[["gasto_id", "Gasto", "Categoria", *month_labels]].reset_index(
            drop=True
        )
    return table_df[["Gasto", "Categoria", *month_labels]].reset_index(drop=True)


@st.cache_data
def _build_ingresos_por_mes_table(
    ingresos_df: pd.DataFrame,
    year: int,
    ingresos_mensuales_df: pd.DataFrame | None = None,
    include_ingreso_id: bool = False,
) -> pd.DataFrame:
    if ingresos_df.empty:
        return pd.DataFrame()
    month_labels = [_month_label(month) for month in range(1, 13)]
    ingresos_mensuales_map = (
        _ingresos_mensuales_map_for_year(ingresos_mensuales_df, year)
        if ingresos_mensuales_df is not None
        else {}
    )
    rows = []
    for _, row in ingresos_df.iterrows():
        row_data = {
            "ingreso_id": int(row["ingreso_id"]),
            "Ingreso": str(row["nombre"]),
            "Periodicidad": str(row["periodicidad"]),
        }
        for month in range(1, 13):
            row_data[_month_label(month)] = _monto_ingreso_for_month(
                row,
                year,
                month,
                ingresos_mensuales_map,
            )
        rows.append(row_data)
    table_df = pd.DataFrame(rows)
    if include_ingreso_id:
        return table_df[["ingreso_id", "Ingreso", "Periodicidad", *month_labels]]
    return table_df[["Ingreso", "Periodicidad", *month_labels]]


@log_time
def main() -> None:
    st.set_page_config(page_title="Presupuesto Familiar", layout="wide")
    st.title("Presupuesto familiar")
    # Mostrar estado de AgGrid para facilitar debugging
    try:
        aggrid_status = "Disponible" if _AGGRID_AVAILABLE else "No disponible"
    except NameError:
        aggrid_status = "No disponible"
    st.sidebar.info(f"AgGrid: {aggrid_status}  {'v' + _AGGRID_VERSION if _AGGRID_AVAILABLE and _AGGRID_VERSION else ''}")

    data = _load_data(DATA_FILE)
    gastos_df = data["gastos"].copy()
    pagos_df = data["pagos"].copy()
    ingresos_df = data["ingresos"].copy()
    cuenta_df = data["cuenta"].copy()
    gastos_mensuales_df = data["gastos_mensuales"].copy()
    ingresos_mensuales_df = data["ingresos_mensuales"].copy()
    comentarios_df = data.get("comentarios", _empty_comentarios_df()).copy()
    gastos_mensuales_df, ingresos_mensuales_df, did_migrate = _migrate_mensuales_from_base(
        gastos_df,
        ingresos_df,
        gastos_mensuales_df,
        ingresos_mensuales_df,
    )
    if did_migrate:
        data["gastos_mensuales"] = gastos_mensuales_df
        data["ingresos_mensuales"] = ingresos_mensuales_df
        _save_data(DATA_FILE, data)

    menu = st.sidebar.radio(
        "Menu",
        [
            "Panel de Gastos",
            "Resumen",
            "Balance",
        ],
    )

    if menu == "Panel de Gastos":
        with st.expander("Registrar gasto", expanded=False):
            with st.form("form_gasto"):
                nombre = st.text_input("Nombre del gasto")
                categoria = st.selectbox("Categoria del gasto", CATEGORIAS)
                monto_presupuestado = st.number_input(
                    "Monto presupuestado",
                    min_value=0.0,
                    step=100.0,
                )
                periodicidad = st.selectbox("Periodicidad", ["Mensual", "Anual"])
                if periodicidad == "Mensual":
                    fecha_pago = st.number_input(
                        "Dia de pago (1-31)",
                        min_value=1,
                        max_value=31,
                        step=1,
                    )
                else:
                    fecha_pago = st.date_input("Fecha de pago")
                fecha_inicio = st.date_input("Fecha de inicio", value=date.today())
                fecha_termino = st.date_input("Fecha de termino", value=date.today())
                submitted = st.form_submit_button("Guardar gasto")

            if submitted:
                if not _normalize_text(nombre):
                    st.error("El nombre del gasto es obligatorio.")
                else:
                    gasto_id = _next_id(gastos_df["gasto_id"])
                    new_row = {
                        "gasto_id": gasto_id,
                        "nombre": _normalize_text(nombre),
                        "categoria": categoria,
                        "monto_presupuestado": float(monto_presupuestado),
                        "periodicidad": periodicidad,
                        "fecha_pago": fecha_pago,
                        "fecha_inicio": fecha_inicio,
                        "fecha_termino": fecha_termino,
                    }
                    gastos_df = pd.concat(
                        [gastos_df, pd.DataFrame([new_row])], ignore_index=True
                    )
                    data["gastos"] = gastos_df
                    gastos_mensuales_df = _append_gasto_mensual_entries(
                        gastos_mensuales_df,
                        new_row,
                    )
                    data["gastos_mensuales"] = gastos_mensuales_df
                    _save_data(DATA_FILE, data)
                    st.success("Gasto registrado.")

        st.subheader("Registrar ingreso")
        with st.expander("Registrar ingreso", expanded=False):
            with st.form("form_ingreso"):
                nombre = st.text_input("Nombre del ingreso")
                monto = st.number_input("Monto del ingreso", min_value=0.0, step=100.0)
                periodicidad = st.selectbox("Periodicidad", ["Mensual", "Anual"], key="period_ing")
                if periodicidad == "Mensual":
                    fecha_pago = st.number_input(
                        "Dia de pago (1-31)",
                        min_value=1,
                        max_value=31,
                        step=1,
                        key="dia_pago_ing",
                    )
                else:
                    fecha_pago = st.date_input("Fecha de pago", key="fecha_pago_ing")
                fecha_inicio = st.date_input("Fecha de inicio", value=date.today(), key="inicio_ing")
                fecha_termino = st.date_input("Fecha de termino", value=date.today(), key="termino_ing")
                submitted = st.form_submit_button("Guardar ingreso")

            if submitted:
                if not _normalize_text(nombre):
                    st.error("El nombre del ingreso es obligatorio.")
                else:
                    ingreso_id = _next_id(ingresos_df["ingreso_id"])
                    new_ingreso = {
                        "ingreso_id": ingreso_id,
                        "nombre": _normalize_text(nombre),
                        "monto": float(monto),
                        "periodicidad": periodicidad,
                        "fecha_pago": fecha_pago,
                        "fecha_inicio": fecha_inicio,
                        "fecha_termino": fecha_termino,
                    }
                    ingresos_df = pd.concat(
                        [ingresos_df, pd.DataFrame([new_ingreso])], ignore_index=True
                    )
                    data["ingresos"] = ingresos_df
                    ingresos_mensuales_df = _append_ingreso_mensual_entries(
                        ingresos_mensuales_df,
                        new_ingreso,
                    )
                    data["ingresos_mensuales"] = ingresos_mensuales_df
                    _save_data(DATA_FILE, data)
                    st.success("Ingreso registrado.")

        current_year, current_month = _current_month()
        if current_month == 1:
            cierre_year = current_year - 1
            cierre_month = 12
        else:
            cierre_year = current_year
            cierre_month = current_month - 1
        months = _month_options()
        month_labels = [label for label, _ in months]
        month_values = [value for _, value in months]
        year_options = list(range(current_year - 2, current_year + 3))
        selected_year = st.selectbox(
            "Anio",
            year_options,
            index=year_options.index(current_year),
            key="gastos_por_mes_anio",
        )

        st.subheader("Gastos presupuestados por mes")
        if gastos_df.empty:
            st.info("No hay gastos registrados.")
        else:
            editable_table = _build_gastos_por_mes_table(
                gastos_df,
                selected_year,
                gastos_mensuales_df,
                include_gasto_id=True,
            ).set_index("gasto_id")
            month_labels = [_month_label(month) for month in range(1, 13)]
            editable_display = _format_amount_columns(editable_table, month_labels)
            editable_display.insert(0, "Eliminar", False)

            tab_gastos_editable, tab_gastos_aggrid = st.tabs(
                ["Editor de Gastos", "Plug Excel"]
            )
            with tab_gastos_editable:
                editable_ajustes_df = st.data_editor(
                    editable_display,
                    use_container_width=True,
                    hide_index=True,
                    disabled=["Gasto", "Categoria"],
                    column_config={
                        "Eliminar": st.column_config.CheckboxColumn(
                            "Eliminar",
                            help="Marca para eliminar el gasto completo",
                        )
                    },
                    key="ajustes_mes_editor",
                )
                confirmar_eliminacion = st.checkbox(
                    "Confirmar eliminacion de gastos seleccionados",
                    key="confirm_del_gasto_anual",
                )
                if st.button(
                    "Eliminar seleccionados",
                    disabled=not confirmar_eliminacion,
                    key="delete_gastos_anual",
                ):
                    delete_ids = (
                        editable_ajustes_df[editable_ajustes_df["Eliminar"] == True]
                        .index.astype(int)
                        .tolist()
                    )
                    if not delete_ids:
                        st.info("No hay gastos marcados para eliminar.")
                    else:
                        gastos_df = gastos_df[~gastos_df["gasto_id"].isin(delete_ids)]
                        pagos_df = pagos_df[~pagos_df["gasto_id"].isin(delete_ids)]
                        if not gastos_mensuales_df.empty:
                            gastos_mensuales_df = gastos_mensuales_df[
                                ~gastos_mensuales_df["gasto_id"].isin(delete_ids)
                            ]
                        data["gastos"] = gastos_df
                        data["pagos"] = pagos_df
                        data["gastos_mensuales"] = gastos_mensuales_df
                        _save_data(DATA_FILE, data)
                        st.success("Gastos eliminados.")
                        st.rerun()
                if st.button("Guardar montos del anio"):
                    new_rows = []
                    for gasto_id, row in editable_ajustes_df.iterrows():
                        for month in range(1, 13):
                            label = _month_label(month)
                            edited_val = _parse_amount(row.get(label))
                            new_rows.append(
                                {
                                    "gasto_id": int(gasto_id),
                                    "year": int(selected_year),
                                    "month": int(month),
                                    "monto_presupuestado": float(edited_val),
                                }
                            )

                    if gastos_mensuales_df.empty:
                        other_rows = _empty_gastos_mensuales_df()
                    else:
                        other_rows = gastos_mensuales_df[
                            gastos_mensuales_df["year"].astype(int) != int(selected_year)
                        ]
                    gastos_mensuales_df = pd.concat(
                        [other_rows, pd.DataFrame(new_rows)], ignore_index=True
                    )
                    if gastos_mensuales_df.empty:
                        gastos_mensuales_df = _empty_gastos_mensuales_df()
                    data["gastos_mensuales"] = gastos_mensuales_df
                    _save_data(DATA_FILE, data)
                    refreshed_table = _build_gastos_por_mes_table(
                        gastos_df,
                        selected_year,
                        gastos_mensuales_df,
                        include_gasto_id=True,
                    ).set_index("gasto_id")
                    refreshed_display = _format_amount_columns(
                        refreshed_table,
                        [_month_label(month) for month in range(1, 13)],
                    )
                    st.rerun()
                    st.success("Montos guardados.")
            with tab_gastos_aggrid:
                aggrid_df = editable_table.reset_index()
                _render_aggrid_sum_view(aggrid_df, key="aggrid_gastos_mes")

        st.subheader("Ingresos presupuestados por mes")
        if ingresos_df.empty:
            st.info("No hay ingresos registrados.")
        else:
            ingresos_editable = _build_ingresos_por_mes_table(
                ingresos_df,
                selected_year,
                ingresos_mensuales_df,
                include_ingreso_id=True,
            ).set_index("ingreso_id")
            ingresos_month_labels = [_month_label(month) for month in range(1, 13)]
            ingresos_display = _format_amount_columns(ingresos_editable, ingresos_month_labels)

            tab_ing_editable, tab_ing_aggrid = st.tabs(
                ["Editor de Ingresos", "Plug Excel"]
            )
            with tab_ing_editable:
                ingresos_editor_df = st.data_editor(
                    ingresos_display,
                    use_container_width=True,
                    hide_index=True,
                    disabled=["Ingreso", "Periodicidad"],
                    key="ingresos_mes_editor",
                )
                if st.button("Guardar montos de ingresos del anio"):
                    new_rows = []
                    for ingreso_id, row in ingresos_editor_df.iterrows():
                        for month in range(1, 13):
                            label = _month_label(month)
                            edited_val = _parse_amount(row.get(label))
                            new_rows.append(
                                {
                                    "ingreso_id": int(ingreso_id),
                                    "year": int(selected_year),
                                    "month": int(month),
                                    "monto": float(edited_val),
                                }
                            )

                    if ingresos_mensuales_df.empty:
                        other_rows = _empty_ingresos_mensuales_df()
                    else:
                        other_rows = ingresos_mensuales_df[
                            ingresos_mensuales_df["year"].astype(int) != int(selected_year)
                        ]
                    ingresos_mensuales_df = pd.concat(
                        [other_rows, pd.DataFrame(new_rows)], ignore_index=True
                    )
                    if ingresos_mensuales_df.empty:
                        ingresos_mensuales_df = _empty_ingresos_mensuales_df()
                    data["ingresos_mensuales"] = ingresos_mensuales_df
                    _save_data(DATA_FILE, data)
                    refreshed_table = _build_ingresos_por_mes_table(
                        ingresos_df,
                        selected_year,
                        ingresos_mensuales_df,
                        include_ingreso_id=True,
                    ).set_index("ingreso_id")
                    refreshed_display = _format_amount_columns(
                        refreshed_table,
                        [_month_label(month) for month in range(1, 13)],
                    )
                    st.rerun()
                    st.success("Montos de ingresos guardados.")
            with tab_ing_aggrid:
                aggrid_df = ingresos_editable.reset_index()
                _render_aggrid_sum_view(aggrid_df, key="aggrid_ingresos_mes")

        st.subheader("Registrar pagos del mes desde esta tabla")
        col_year_pagos, col_month_pagos = st.columns(2)
        with col_year_pagos:
            selected_year_pagos = st.selectbox(
                "Anio",
                year_options,
                index=year_options.index(cierre_year),
                key="pagos_mes_anio",
            )
        with col_month_pagos:
            selected_month_label_pagos = st.selectbox(
                "Mes",
                month_labels,
                index=cierre_month - 1,
                key="pagos_mes_mes",
            )
        selected_month_pagos = month_values[month_labels.index(selected_month_label_pagos)]
        gastos_mes = []
        gastos_mensuales_map = _gastos_mensuales_map_for_year(
            gastos_mensuales_df,
            selected_year_pagos,
        )
        for _, row in gastos_df.iterrows():
            monto_mes = _presupuesto_for_month(
                row,
                selected_year_pagos,
                selected_month_pagos,
                gastos_mensuales_map,
            )
            if monto_mes == 0.0:
                continue
            row_data = row.copy()
            row_data["monto_presupuestado_mes"] = monto_mes
            gastos_mes.append(row_data)

        if not gastos_mes:
            st.info("No hay gastos presupuestados para ese mes.")
        else:
            unpaid_rows = []
            paid_rows = []
            for _, row in pd.DataFrame(gastos_mes).iterrows():
                gasto_id = int(row["gasto_id"])
                base = {
                    "gasto_id": gasto_id,
                    "nombre": str(row["nombre"]),
                    "categoria": str(row["categoria"]),
                    "monto_presupuestado": float(row["monto_presupuestado_mes"]),
                }
                existing_pago = _get_pago_for_month(
                    pagos_df,
                    gasto_id,
                    selected_year_pagos,
                    selected_month_pagos,
                )
                if existing_pago is None:
                    unpaid_rows.append(
                        {
                            **base,
                            "monto_real": float(row["monto_presupuestado_mes"]),
                            "fecha_pago_real": date(selected_year_pagos, selected_month_pagos, 1),
                            "pagar": False,
                        }
                    )
                else:
                    paid_rows.append(
                        {
                            **base,
                            "monto_real": float(existing_pago["monto_real"]),
                            "fecha_pago_real": _parse_date(existing_pago["fecha_pago_real"])
                            or date.today(),
                            "estado": str(existing_pago.get("estado", "Pagado")),
                        }
                    )

            if paid_rows:
                st.caption("Pagos ya registrados para ese mes.")
                paid_df = pd.DataFrame(paid_rows)[
                    [
                        "nombre",
                        "categoria",
                        "monto_presupuestado",
                        "monto_real",
                        "fecha_pago_real",
                        "estado",
                    ]
                ]
                paid_df = _sort_by_categoria_nombre(paid_df)
                paid_display = _format_amount_columns(
                    paid_df,
                    ["monto_presupuestado", "monto_real"],
                )
                st.dataframe(paid_display, use_container_width=True)

            if unpaid_rows:
                unpaid_df = pd.DataFrame(unpaid_rows).set_index("gasto_id")
                unpaid_df = _sort_by_categoria_nombre(
                    unpaid_df.reset_index(),
                ).set_index("gasto_id")
                unpaid_display = _format_amount_columns(
                    unpaid_df,
                    ["monto_presupuestado", "monto_real"],
                )
                pagos_editor_key = f"pagos_mes_editor_{selected_year_pagos}_{selected_month_pagos}"
                editable_df = st.data_editor(
                    unpaid_display,
                    use_container_width=True,
                    hide_index=True,
                    disabled=["nombre", "categoria", "monto_presupuestado"],
                    column_config={
                        "fecha_pago_real": st.column_config.DateColumn(
                            "Fecha pago",
                            format="YYYY-MM-DD",
                        )
                    },
                    key=pagos_editor_key,
                )

                if st.button("Guardar pagos del mes"):
                    to_register = editable_df[editable_df["pagar"] == True]
                    if to_register.empty:
                        st.info("No hay pagos marcados para registrar.")
                    else:
                        next_id = _next_id(pagos_df["pago_id"])
                        new_rows = []
                        skipped = []
                        for _, row in to_register.reset_index().iterrows():
                            gasto_id = int(row["gasto_id"])
                            if (
                                _get_pago_for_month(
                                    pagos_df,
                                    gasto_id,
                                    selected_year_pagos,
                                    selected_month_pagos,
                                )
                                is not None
                            ):
                                skipped.append(str(row["nombre"]))
                                continue
                            monto_real = _parse_amount(row["monto_real"])
                            parsed_fecha = _parse_date(row["fecha_pago_real"])
                            if parsed_fecha is None:
                                fecha_pago_real = date(
                                    selected_year_pagos,
                                    selected_month_pagos,
                                    1,
                                )
                            else:
                                if (
                                    parsed_fecha.year != selected_year_pagos
                                    or parsed_fecha.month != selected_month_pagos
                                ):
                                    last_day = calendar.monthrange(
                                        selected_year_pagos,
                                        selected_month_pagos,
                                    )[1]
                                    adjusted_day = min(parsed_fecha.day, last_day)
                                    fecha_pago_real = date(
                                        selected_year_pagos,
                                        selected_month_pagos,
                                        adjusted_day,
                                    )
                                else:
                                    fecha_pago_real = parsed_fecha
                            new_rows.append(
                                {
                                    "pago_id": next_id,
                                    "gasto_id": gasto_id,
                                    "monto_real": monto_real,
                                    "fecha_pago_real": fecha_pago_real,
                                    "estado": "Pagado",
                                }
                            )
                            next_id += 1

                        if new_rows:
                            pagos_df = pd.concat(
                                [pagos_df, pd.DataFrame(new_rows)], ignore_index=True
                            )
                            data["pagos"] = pagos_df
                            _save_data(DATA_FILE, data)
                            st.success("Pagos registrados.")

                        if skipped:
                            st.warning(
                                "Ya existe un pago para estos gastos en el mes seleccionado: "
                                + ", ".join(skipped)
                            )
            else:
                st.info("No hay pagos pendientes para registrar en ese mes.")

    if menu == "Resumen":
        current_year, current_month = _current_month()
        if current_month == 1:
            cierre_year = current_year - 1
            cierre_month = 12
        else:
            cierre_year = current_year
            cierre_month = current_month - 1
        months = _month_options()
        month_labels = [label for label, _ in months]
        month_values = [value for _, value in months]
        st.subheader("Resumen del mes")
        col_year, col_month = st.columns(2)
        with col_year:
            selected_year_monthly = st.selectbox(
                "Anio",
                list(range(current_year - 2, current_year + 3)),
                index=list(range(current_year - 2, current_year + 3)).index(cierre_year),
                key="resumen_anio",
            )
        with col_month:
            selected_month_label = st.selectbox(
                "Mes",
                month_labels,
                index=cierre_month - 1,
                key="resumen_mes",
            )
        selected_month = month_values[month_labels.index(selected_month_label)]

        gastos_mensuales_map = _gastos_mensuales_map_for_year(
            gastos_mensuales_df,
            selected_year_monthly,
        )
        total_gastos_presupuestados = 0.0
        for _, row in gastos_df.iterrows():
            total_gastos_presupuestados += _presupuesto_for_month(
                row,
                selected_year_monthly,
                selected_month,
                gastos_mensuales_map,
            )
        total_gastos_reales = 0.0
        if not pagos_df.empty:
            for _, row in pagos_df.iterrows():
                fecha_pago = _parse_date(row.get("fecha_pago_real"))
                if not fecha_pago:
                    continue
                if fecha_pago.year != selected_year_monthly or fecha_pago.month != selected_month:
                    continue
                total_gastos_reales += float(row.get("monto_real", 0.0))

        total_pendiente = max(total_gastos_presupuestados - total_gastos_reales, 0.0)

        st.markdown(
            f"Este mes estaba presupuestado gastar {_format_amount(total_gastos_presupuestados)} "
            f"y a la fecha se ha gastado {_format_amount(total_gastos_reales)}."
        )
        st.markdown(
            f"Falta por pagar {_format_amount(total_pendiente)} este mes."
        )

        st.subheader("Resumen anual")
        year_options = list(range(current_year - 2, current_year + 3))
        selected_year = st.selectbox(
            "Anio",
            year_options,
            index=year_options.index(current_year),
        )
        annual_rows = []
        gastos_mensuales_map = _gastos_mensuales_map_for_year(
            gastos_mensuales_df,
            selected_year,
        )
        ingresos_mensuales_map = _ingresos_mensuales_map_for_year(
            ingresos_mensuales_df,
            selected_year,
        )
        gastos_reales_map = {month: 0.0 for month in range(1, 13)}
        if not pagos_df.empty:
            for _, row in pagos_df.iterrows():
                fecha_pago = _parse_date(row.get("fecha_pago_real"))
                if not fecha_pago:
                    continue
                if fecha_pago.year != selected_year:
                    continue
                gastos_reales_map[fecha_pago.month] += float(row.get("monto_real", 0.0))

        for month in range(1, 13):
            total_gastos_mes = 0.0
            for _, row in gastos_df.iterrows():
                total_gastos_mes += _presupuesto_for_month(
                    row,
                    selected_year,
                    month,
                    gastos_mensuales_map,
                )
            total_ingresos_mes = 0.0
            for _, row in ingresos_df.iterrows():
                total_ingresos_mes += _monto_ingreso_for_month(
                    row,
                    selected_year,
                    month,
                    ingresos_mensuales_map,
                )
            total_gastos_reales_mes = float(gastos_reales_map.get(month, 0.0))

            annual_rows.append(
                {
                    "Mes": _month_label(month),
                    "Ingresos": total_ingresos_mes,
                    "Gastos presupuestados": total_gastos_mes,
                    "Gastos reales": total_gastos_reales_mes,
                    "Balance": total_ingresos_mes - total_gastos_mes,
                }
            )

        annual_df = pd.DataFrame(annual_rows)
        annual_pivot = annual_df.set_index("Mes").T
        annual_pivot_display = _format_amount_columns(
            annual_pivot,
            [label for label, _ in months],
        )
        tab_res_normal, tab_res_aggrid = st.tabs(
            ["Tabla normal", "Sumar seleccion (AgGrid)"]
        )
        with tab_res_normal:
            st.dataframe(annual_pivot_display, use_container_width=True)
        with tab_res_aggrid:
            _render_aggrid_sum_view(annual_pivot.reset_index(), key="aggrid_resumen_anual")
        chart_df = annual_df.melt(
            id_vars=["Mes"],
            value_vars=["Ingresos", "Gastos presupuestados", "Gastos reales"],
            var_name="Serie",
            value_name="Valor",
        )
        balance_df = annual_df[["Mes", "Balance"]].copy()
        bars = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("Mes:N", sort=month_labels),
                xOffset="Serie:N",
                y=alt.Y("Valor:Q"),
                color=alt.Color(
                    "Serie:N",
                    scale=alt.Scale(
                        domain=["Ingresos", "Gastos presupuestados", "Gastos reales"],
                        range=["#66bb6a", "#ef9a9a", "#4fc3f7"],
                    ),
                ),
            )
        )
        balance_line = (
            alt.Chart(balance_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Mes:N", sort=month_labels),
                y=alt.Y("Balance:Q"),
                color=alt.value("#049b0e"),
                tooltip=["Mes", "Balance"],
            )
        )
        st.altair_chart(bars + balance_line, use_container_width=True)

        st.subheader("Comentarios")
        comentario_actual = ""
        if not comentarios_df.empty and "comentario" in comentarios_df.columns:
            comentario_actual = str(comentarios_df.iloc[0].get("comentario", ""))
        comentario_input = st.text_area(
            "",
            value=comentario_actual,
            height=140,
            placeholder="Escribe tus comentarios del resumen...",
            key="resumen_comentarios",
        )
        if st.button("Guardar comentario"):
            comentarios_df = pd.DataFrame(
                [{"comentario": comentario_input.strip()}]
            )
            data["comentarios"] = comentarios_df
            _save_data(DATA_FILE, data)
            st.success("Comentario guardado.")

    if menu == "Balance":
        st.subheader("Saldo cuenta corriente")
        saldo_actual = _load_or_init_saldo(cuenta_df)
        saldo_input = st.number_input(
            "Saldo actual",
            min_value=0.0,
            step=1000.0,
            value=float(saldo_actual),
        )
        if st.button("Guardar saldo"):
            cuenta_df = _set_saldo(cuenta_df, float(saldo_input))
            data["cuenta"] = cuenta_df
            _save_data(DATA_FILE, data)
            st.success("Saldo actualizado.")

        year, month = _current_month()
        gastos_mensuales_map = _gastos_mensuales_map_for_year(
            gastos_mensuales_df,
            year,
        )
        ingresos_mensuales_map = _ingresos_mensuales_map_for_year(
            ingresos_mensuales_df,
            year,
        )
        balance_restante = 0.0
        for month_idx in range(month, 13):
            total_gastos_mes = 0.0
            for _, row in gastos_df.iterrows():
                total_gastos_mes += _presupuesto_for_month(
                    row,
                    year,
                    month_idx,
                    gastos_mensuales_map,
                )
            total_ingresos_mes = 0.0
            for _, row in ingresos_df.iterrows():
                total_ingresos_mes += _monto_ingreso_for_month(
                    row,
                    year,
                    month_idx,
                    ingresos_mensuales_map,
                )
            balance_restante += total_ingresos_mes - total_gastos_mes

        saldo_proyectado = float(saldo_input) + balance_restante
        st.markdown(
            f"Tu balance para el {year}, corresponde a:"
        )
        st.markdown(
            f"{_format_amount(saldo_input)} + {_format_amount(balance_restante)} = "
            f"{_format_amount(saldo_proyectado)}"
        )

        next_year = year + 1
        gastos_mensuales_map_next = _gastos_mensuales_map_for_year(
            gastos_mensuales_df,
            next_year,
        )
        ingresos_mensuales_map_next = _ingresos_mensuales_map_for_year(
            ingresos_mensuales_df,
            next_year,
        )
        balance_next_year = 0.0
        for month_idx in range(1, 13):
            total_gastos_mes = 0.0
            for _, row in gastos_df.iterrows():
                total_gastos_mes += _presupuesto_for_month(
                    row,
                    next_year,
                    month_idx,
                    gastos_mensuales_map_next,
                )
            total_ingresos_mes = 0.0
            for _, row in ingresos_df.iterrows():
                total_ingresos_mes += _monto_ingreso_for_month(
                    row,
                    next_year,
                    month_idx,
                    ingresos_mensuales_map_next,
                )
            balance_next_year += total_ingresos_mes - total_gastos_mes

        st.markdown(
            f"Tu balance para el {next_year}, corresponde a:"
        )
        st.markdown(
            f"{_format_amount(balance_next_year)}"
        )
        gastos_pendientes = []
        for _, row in gastos_df.iterrows():
            monto_mes = _presupuesto_for_month(row, year, month, gastos_mensuales_map)
            if monto_mes == 0.0:
                continue
            gasto_id = int(row["gasto_id"])
            if _paid_in_month(pagos_df, gasto_id, year, month):
                continue
            row_data = row.copy()
            row_data["monto_presupuestado"] = monto_mes
            gastos_pendientes.append(row_data)

        if gastos_pendientes:
            pendientes_df = _sort_by_categoria_nombre(pd.DataFrame(gastos_pendientes))
            total_pendiente = float(pendientes_df["monto_presupuestado"].sum())
            saldo_real = float(saldo_input) - total_pendiente
            st.dataframe(
                _format_amount_columns(
                    pendientes_df[["nombre", "categoria", "monto_presupuestado"]],
                    ["monto_presupuestado"],
                ),
                use_container_width=True,
            )
            st.markdown(
                f"**Total pendiente:** {_format_amount(total_pendiente)}  \\n"
                f"**Saldo ajustado:** {_format_amount(saldo_real)}",
                unsafe_allow_html=True,
            )
        else:
            st.info("No hay gastos pendientes este mes.")


if __name__ == "__main__":
    main()
