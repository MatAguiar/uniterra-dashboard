import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px  
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, Input, Output, State, callback_context, dash_table
import dash
import dash_auth
import urllib.request
import io

# 1. LINK DO GOOGLE SHEETS E CREDENCIAIS
LINK_GOOGLE_SHEETS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRu6rVlR1vXhP5Dsb-XuC0j57q8kp8RPJWfmbmB6Hf-fD5HAayoxtGHbhDLe2IngTxSZcoKqcieZsar/pub?gid=1101979435&single=true&output=csv"

# Defina aqui os usuários e senhas para acessar o site
USUARIOS_PERMITIDOS = {
    'matheus': '123456',
    'uniterra': 'frota2026',
    'diretoria': 'acesso10'
}

print("Conectando ao Google Sheets...")

try:
    # -------------------------------------------------------------------------
    # NOVO SISTEMA ANTIBLOQUEIO (Disfarce de Navegador + Timeout)
    # -------------------------------------------------------------------------
    req = urllib.request.Request(
        LINK_GOOGLE_SHEETS, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    )
    # Impõe um limite máximo de 30 segundos para baixar (evita que o Render trave)
    with urllib.request.urlopen(req, timeout=30) as response:
        conteudo_csv = response.read().decode('utf-8-sig') # Lê e remove sujeiras (BOM) do Google
    
    df = pd.read_csv(io.StringIO(conteudo_csv))
    print("Planilha carregada com sucesso!")
except Exception as e:
    print(f"ERRO CRÍTICO: Não foi possível ler a planilha online. Detalhe: {e}")
    df = pd.DataFrame()

if not df.empty:
    # Limpeza radical de caracteres fantasmas e espaços invisíveis nos cabeçalhos
    df.columns = [str(c).strip().upper().replace('\ufeff', '').replace('Ï»¿', '') for c in df.columns]
    
    coluna_km = 'HOR/KM ATUAL' 
    coluna_litros = 'QUANT COMB'
    coluna_mes = 'MÊS REF' 
    coluna_cat = 'CATEGORIA' 

    for col in [coluna_litros, coluna_km]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce') 
    
    # =========================================================================
    # SOLUÇÃO DEFINITIVA PARA A DATA (Força Bruta)
    # =========================================================================
    # Pega a primeira coluna do arquivo (índice 0) e força ela a ser a DATA,
    # ignorando qualquer erro de digitação do almoxarife no cabeçalho!
    if len(df.columns) > 0:
        nome_primeira_coluna = df.columns[0]
        df.rename(columns={nome_primeira_coluna: 'DATA'}, inplace=True)

    df['DATA'] = pd.to_datetime(df['DATA'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['DATA'])

    if coluna_mes in df.columns:
        df[coluna_mes] = df[coluna_mes].astype(str)
        df['DATA_REF'] = pd.to_datetime(df[coluna_mes], errors='coerce')
        df['MES_STR'] = df['DATA_REF'].dt.strftime('%m/%Y') 
        df = df.dropna(subset=['DATA_REF'])

    df = df.sort_values(by=['MAQUINA', 'DATA'])
    
    if coluna_km in df.columns and coluna_litros in df.columns:
        df['KM_VALIDO'] = df[coluna_km].replace(0, np.nan)
        df['REF_ANTERIOR'] = df.groupby('MAQUINA')['KM_VALIDO'].transform(lambda x: x.ffill().shift(1))
        df['CONSUMO'] = (df['KM_VALIDO'] - df['REF_ANTERIOR']) / df[coluna_litros]
        df['CONSUMO'] = df['CONSUMO'].replace([np.inf, -np.inf], np.nan)
        
        LIMITE_MAXIMO_CONSUMO = 25 
        df.loc[(df['CONSUMO'] <= 0) | (df['CONSUMO'] > LIMITE_MAXIMO_CONSUMO), 'CONSUMO'] = np.nan
    
    if coluna_cat in df.columns:
        df[coluna_cat] = df[coluna_cat].fillna('Outros').astype(str)
    else:
        df[coluna_cat] = 'Geral'

    if 'MAQUINA' in df.columns:
        lista_categorias = sorted(df[coluna_cat].unique())
        lista_maquinas = sorted(df['MAQUINA'].unique())
        
        maquinas_cat_df = df[['MAQUINA', coluna_cat]].drop_duplicates().sort_values(by=[coluna_cat, 'MAQUINA'])
        opcoes_drop_maquina = [{'label': f"{row[coluna_cat]} - {row['MAQUINA']}", 'value': row['MAQUINA']} for idx, row in maquinas_cat_df.iterrows()]
    else:
        lista_categorias, lista_maquinas, opcoes_drop_maquina = [], [], []

    if 'DATA_REF' in df.columns:
        todos_os_meses = sorted(df['DATA_REF'].unique())
        N = len(todos_os_meses)
    else:
        todos_os_meses, N = [], 0
else:
    lista_categorias, lista_maquinas, todos_os_meses, N = [], [], [], 0
    opcoes_drop_maquina = []

# =========================================================================
# INICIAR O APLICATIVO DASH (WEB APP)
# =========================================================================
app = Dash(__name__, title="Uniterra - Frota")

auth = dash_auth.BasicAuth(app, USUARIOS_PERMITIDOS)
server = app.server 

estilo_caixa = {'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '8px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)', 'marginBottom': '30px'}

app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'backgroundColor': '#ECEFF1', 'padding': '20px', 'margin': '0'}, children=[
    html.Div(style={'backgroundColor': '#2C3E50', 'color': 'white', 'padding': '15px', 'borderRadius': '8px', 'marginBottom': '20px', 'textAlign': 'center', 'boxShadow': '0 4px 6px rgba(0,0,0,0.2)'}, children=[
        html.H1("Controle de Combustível - Uniterra", style={'margin': '0'})
    ]),
    
    html.Div(style=estilo_caixa, children=[
        html.H2("1. Visão Geral da Empresa", style={'marginTop': '0', 'color': '#34495E'}),
        html.Div(style={'backgroundColor': '#F8F9F9', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px'}, children=[
            
            html.Strong("Filtro de Categoria:"),
            html.Div(style={'marginBottom': '10px', 'marginTop': '5px'}, children=[
                html.Button('Selecionar Todas', id='btn-todas-cat', n_clicks=0, style={'marginRight': '10px', 'cursor': 'pointer', 'backgroundColor': '#3498DB', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'}),
                html.Button('Limpar Seleção', id='btn-nenhuma-cat', n_clicks=0, style={'cursor': 'pointer', 'backgroundColor': '#95A5A6', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'})
            ]),
            dcc.Checklist(
                id='check-categoria',
                options=[{'label': cat, 'value': cat} for cat in lista_categorias],
                value=lista_categorias, 
                inline=True,
                inputStyle={'marginRight': '5px', 'marginLeft': '15px'},
                style={'display': 'flex', 'flexWrap': 'wrap', 'marginBottom': '20px'}
            ),

            html.Strong("Filtro de Máquinas:"),
            html.Div(style={'marginBottom': '10px', 'marginTop': '5px'}, children=[
                html.Button('Selecionar Todas', id='btn-todas', n_clicks=0, style={'marginRight': '10px', 'cursor': 'pointer', 'backgroundColor': '#3498DB', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'}),
                html.Button('Limpar Seleção', id='btn-nenhuma', n_clicks=0, style={'cursor': 'pointer', 'backgroundColor': '#95A5A6', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'})
            ]),
            dcc.Checklist(
                id='check-maquinas', 
                options=[{'label': maq, 'value': maq} for maq in lista_maquinas], 
                value=lista_maquinas, 
                inline=True, 
                inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, 
                style={'display': 'flex', 'flexWrap': 'wrap', 'marginBottom': '15px'}
            ),
            
            html.Strong("Período de Análise:"),
            dcc.RadioItems(id='radio-tempo', options=[{'label': 'Este mês', 'value': 1}, {'label': 'Últimos 3 meses', 'value': 3}, {'label': 'Últimos 6 meses', 'value': 6}, {'label': 'Últimos 12 meses', 'value': 12}, {'label': 'Total disponível', 'value': N}], value=N, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'})
        ]),
        dcc.Graph(id='grafico-geral'),
        html.Div(id='tabela-geral-container', style={'marginTop': '20px'})
    ]),
    
    html.Div(style=estilo_caixa, children=[
        html.H2("2. Análise Detalhada por Máquina", style={'marginTop': '0', 'color': '#34495E'}),
        html.Div(style={'display': 'flex', 'gap': '20px', 'backgroundColor': '#F8F9F9', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px', 'flexWrap': 'wrap'}, children=[
            html.Div(style={'flex': '1', 'minWidth': '300px'}, children=[
                html.Strong("Selecione a Máquina:"),
                dcc.Dropdown(
                    id='drop-maquina', 
                    options=opcoes_drop_maquina, 
                    value=opcoes_drop_maquina[0]['value'] if opcoes_drop_maquina else None, 
                    clearable=False, 
                    style={'marginTop': '5px'}
                )
            ]),
            html.Div(style={'flex': '2', 'minWidth': '300px'}, children=[
                html.Strong("Período:"),
                dcc.RadioItems(id='radio-tempo-maq', options=[{'label': 'Este mês', 'value': 1}, {'label': 'Últimos 3 meses', 'value': 3}, {'label': 'Últimos 6 meses', 'value': 6}, {'label': 'Últimos 12 meses', 'value': 12}, {'label': 'Total disponível', 'value': N}], value=N, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, style={'marginTop': '10px'})
            ])
        ]),
        dcc.Graph(id='grafico-detalhe'),
        html.Div(id='tabela-detalhe-container', style={'marginTop': '20px'})
    ])
])

@app.callback(
    Output('check-categoria', 'value'),
    Input('btn-todas-cat', 'n_clicks'),
    Input('btn-nenhuma-cat', 'n_clicks'),
    prevent_initial_call=True
)
def atualizar_checkboxes_categoria(btn_todas, btn_nenhuma):
    ctx = callback_context
    id_acionado = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None
    if id_acionado == 'btn-todas-cat': return lista_categorias
    elif id_acionado == 'btn-nenhuma-cat': return []
    return dash.no_update

@app.callback(
    Output('check-maquinas', 'options'),
    Output('check-maquinas', 'value'),
    Input('check-categoria', 'value'),
    Input('btn-todas', 'n_clicks'),
    Input('btn-nenhuma', 'n_clicks'),
    State('check-maquinas', 'value'),
    prevent_initial_call=False
)
def atualizar_maquinas_por_categoria(categorias_selec, btn_todas, btn_nenhuma, maquinas_atuais):
    ctx = callback_context
    id_acionado = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    if df.empty: return [], []
    if not categorias_selec: maquinas_disponiveis = []
    else: maquinas_disponiveis = sorted(df[df['CATEGORIA'].isin(categorias_selec)]['MAQUINA'].unique())

    opcoes_maquinas = [{'label': maq, 'value': maq} for maq in maquinas_disponiveis]

    if id_acionado == 'btn-todas': return opcoes_maquinas, maquinas_disponiveis
    elif id_acionado == 'btn-nenhuma': return opcoes_maquinas, []
    
    maquinas_atuais = maquinas_atuais or []
    maquinas_validas = [maq for maq in maquinas_atuais if maq in maquinas_disponiveis]
    if not maquinas_validas and maquinas_disponiveis: maquinas_validas = maquinas_disponiveis
    return opcoes_maquinas, maquinas_validas

# ATUALIZA A VISÃO GERAL
@app.callback(
    Output('grafico-geral', 'figure'),
    Output('tabela-geral-container', 'children'),
    Input('check-maquinas', 'value'), 
    Input('radio-tempo', 'value')
)
def atualizar_visao_geral(maquinas_selec, meses_selec):
    vazio = html.Div() 
    if not maquinas_selec or df.empty: return go.Figure().update_layout(title="Nenhuma máquina selecionada.", template="plotly_white", height=300), vazio
    
    df_filt = df[df['MAQUINA'].isin(maquinas_selec)]
    meses_validos = todos_os_meses[-meses_selec:] if meses_selec > 0 else []
    df_filt = df_filt[df_filt['DATA_REF'].isin(meses_validos)]
    if df_filt.empty: return go.Figure().update_layout(title="Sem dados para este período.", template="plotly_white", height=300), vazio

    lista_meses_texto = sorted(df_filt['MES_STR'].unique(), key=lambda x: pd.to_datetime(x, format='%m/%Y'))
    
    fig = make_subplots(
        rows=2, cols=1, 
        vertical_spacing=0.1, 
        specs=[[{"type": "xy"}], [{"type": "domain"}]], 
        subplot_titles=("Total Consumido (Litros) - Evolução Mensal", "Proporção de Consumo por Máquina no Período")
    )

    df_empresa = df_filt.groupby('MES_STR')['QUANT COMB'].sum().reset_index()
    soma_str = f"{df_empresa['QUANT COMB'].sum():,.0f} Litros".replace(',', '.')
    fig.add_trace(go.Bar(x=df_empresa['MES_STR'], y=df_empresa['QUANT COMB'], text=df_empresa['QUANT COMB'].round(0), textposition='auto', marker_color='#27AE60', name='Total', showlegend=False), row=1, col=1)

    df_resumo_full = df_filt.groupby('MAQUINA').agg({
        'QUANT COMB':'sum', 
        'CONSUMO':'mean',
        'CATEGORIA':'first' 
    }).reset_index().sort_values(by='QUANT COMB', ascending=False)
    
    df_resumo_treemap = df_resumo_full[df_resumo_full['QUANT COMB'] > 0].copy()
    
    if not df_resumo_treemap.empty:
        categorias_unicas = sorted(df_resumo_treemap['CATEGORIA'].unique())
        paleta_cores = px.colors.qualitative.Plotly 
        
        mapa_cores = {cat: paleta_cores[i % len(paleta_cores)] for i, cat in enumerate(categorias_unicas)}
        cores_treemap = df_resumo_treemap['CATEGORIA'].map(mapa_cores).tolist()
        df_resumo_treemap['TEXTO_HOVER'] = "Categoria: " + df_resumo_treemap['CATEGORIA']

        fig.add_trace(go.Treemap(
            labels=df_resumo_treemap['MAQUINA'],
            parents=[""] * len(df_resumo_treemap),
            values=df_resumo_treemap['QUANT COMB'],
            text=df_resumo_treemap['TEXTO_HOVER'], 
            textinfo="label+value+percent root", 
            marker=dict(colors=cores_treemap), 
            hovertemplate="<b>%{label}</b><br>%{text}<br>Consumo Total: %{value:,.0f} Litros<br>Representa: %{percentRoot:.1%} da seleção<extra></extra>"
        ), row=2, col=1)

        for cat in categorias_unicas:
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode='markers', marker=dict(size=15, color=mapa_cores[cat], symbol='square'), name=cat, showlegend=True
            ), row=1, col=1)

    fig.update_layout(title=f"Soma do Período Filtrado: <b>{soma_str}</b>", template="plotly_white", height=800, showlegend=True, margin=dict(t=50))
    fig.update_xaxes(categoryorder='array', categoryarray=lista_meses_texto, row=1, col=1)
    
    tabela_litros = df_resumo_full['QUANT COMB'].apply(lambda x: f"{x:,.0f} L".replace(',', '.'))
    tabela_consumo = df_resumo_full['CONSUMO'].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "S/D")
    
    df_table_geral = pd.DataFrame({
        'Máquina': df_resumo_full['MAQUINA'],
        'Categoria': df_resumo_full['CATEGORIA'],
        'Total (Litros)': tabela_litros,
        'Média Consumo': tabela_consumo
    }).reset_index(drop=True)

    tabela_geral_ui = html.Div([
        html.H3("Matriz de Dados do Período", style={'textAlign': 'center', 'color': '#34495E', 'marginBottom': '15px'}),
        dash_table.DataTable(
            data=df_table_geral.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df_table_geral.columns],
            style_table={'overflowX': 'auto', 'minWidth': '100%'}, 
            style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'},
            style_header={'backgroundColor': '#2C3E50', 'color': 'white', 'fontWeight': 'bold'},
            style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}], 
            page_size=10 
        )
    ])

    return fig, tabela_geral_ui

# ATUALIZA A VISÃO DE DETALHE POR MÁQUINA
@app.callback(
    Output('grafico-detalhe', 'figure'), 
    Output('tabela-detalhe-container', 'children'),
    Input('drop-maquina', 'value'), 
    Input('radio-tempo-maq', 'value')
)
def atualizar_visao_maquina(maq_selec, meses_selec):
    vazio = html.Div()
    if not maq_selec or df.empty: return go.Figure().update_layout(title="Sem dados.", template="plotly_white", height=300), vazio
    
    df_m = df[df['MAQUINA'] == maq_selec]
    meses_validos = todos_os_meses[-meses_selec:] if meses_selec > 0 else []
    df_m = df_m[df_m['DATA_REF'].isin(meses_validos)]
    if df_m.empty: return go.Figure().update_layout(title="Sem dados para esta máquina no período.", template="plotly_white", height=300), vazio

    df_grp = df_m.groupby('MES_STR').agg({'CONSUMO': 'mean', 'QUANT COMB': 'sum'}).reset_index()
    lista_meses_texto = sorted(df_grp['MES_STR'].unique(), key=lambda x: pd.to_datetime(x, format='%m/%Y'))
    
    m_geral = df[df['MAQUINA'] == maq_selec]['CONSUMO'].mean()
    if pd.isna(m_geral): m_geral = 0 

    fig2 = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=False, 
        subplot_titles=("Média de Consumo Mensal (Km/L ou Horas/L)", "Total de Combustível Gasto (Litros)"), 
        vertical_spacing=0.15,
        specs=[[{"type": "xy"}], [{"type": "xy"}]]
    )
    
    fig2.add_trace(go.Bar(x=df_grp['MES_STR'], y=df_grp['CONSUMO'], text=df_grp['CONSUMO'].round(2), textposition='auto', marker_color='#2980B9', name='Consumo'), row=1, col=1)
    
    if m_geral > 0:
        fig2.add_trace(go.Scatter(x=df_grp['MES_STR'], y=[m_geral]*len(df_grp), mode='lines', line=dict(color='#E74C3C', dash='dash'), name='Média Histórica'), row=1, col=1)
        
    fig2.add_trace(go.Bar(x=df_grp['MES_STR'], y=df_grp['QUANT COMB'], text=df_grp['QUANT COMB'].round(0), textposition='auto', marker_color='#F39C12', name='Litros'), row=2, col=1)

    fig2.update_layout(title=f"Desempenho Detalhado: <b>{maq_selec}</b>", template="plotly_white", height=600, showlegend=False)
    fig2.update_xaxes(categoryorder='array', categoryarray=lista_meses_texto, row=1, col=1)
    fig2.update_xaxes(categoryorder='array', categoryarray=lista_meses_texto, row=2, col=1)
    
    # === CRIAÇÃO DA TABELA NATIVA (HISTÓRICO) ===
    df_table = df_m[['DATA', 'QUANT COMB', 'CONSUMO']].copy()
    
    df_table = df_table.sort_values(by='DATA', ascending=False).reset_index(drop=True)
    
    data_str = df_table['DATA'].dt.strftime('%d/%m/%Y').fillna('Sem Data')
    litros_str = df_table['QUANT COMB'].apply(lambda x: f"{x:,.0f} L".replace(',', '.'))
    consumo_str = df_table['CONSUMO'].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "S/D")

    df_table_html = pd.DataFrame({
        'Data do Abastecimento': data_str,
        'Qtd. Abastecida (Litros)': litros_str,
        'Consumo Registrado (Km/L)': consumo_str
    })

    tabela_detalhe_ui = html.Div([
        html.H3("Histórico de Abastecimentos (Lançamentos)", style={'textAlign': 'center', 'color': '#34495E', 'marginBottom': '15px'}),
        dash_table.DataTable(
            data=df_table_html.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df_table_html.columns],
            style_table={'overflowX': 'auto', 'minWidth': '100%'}, 
            style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'},
            style_header={'backgroundColor': '#2C3E50', 'color': 'white', 'fontWeight': 'bold'},
            style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}], 
            page_size=10 
        )
    ])
    
    return fig2, tabela_detalhe_ui

if __name__ == "__main__":
    print("\n" + "="*50)
    print("SERVIDOR WEB INICIADO! (Sistema Antibloqueio Ativado)")
    print("Acesse o seu navegador e digite: http://127.0.0.1:8050")
    print("="*50 + "\n")
    app.run(debug=False, use_reloader=False)
