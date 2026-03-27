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
import os
import socket

# =========================================================================
# PROTEÇÃO CONTRA TRAVAMENTOS E TIMEOUT
# =========================================================================
socket.setdefaulttimeout(15)

# 1. LINKS DO GOOGLE SHEETS E CREDENCIAIS
LINK_GOOGLE_SHEETS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRu6rVlR1vXhP5Dsb-XuC0j57q8kp8RPJWfmbmB6Hf-fD5HAayoxtGHbhDLe2IngTxSZcoKqcieZsar/pub?gid=1101979435&single=true&output=csv"
LINK_ENTRADA_DIESEL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRu6rVlR1vXhP5Dsb-XuC0j57q8kp8RPJWfmbmB6Hf-fD5HAayoxtGHbhDLe2IngTxSZcoKqcieZsar/pub?gid=2114856858&single=true&output=csv"

USUARIOS_PERMITIDOS = {
    'matheus': '123456',
    'uniterra': 'frota2026',
    'diretoria': 'acesso10'
}

# =========================================================================
# INICIAR O APLICATIVO DASH
# =========================================================================
app = Dash(__name__, title="Uniterra - Frota")
auth = dash_auth.BasicAuth(app, USUARIOS_PERMITIDOS)
server = app.server 
server.secret_key = os.urllib.request.pathname2url(LINK_GOOGLE_SHEETS) if hasattr(os, 'urllib') else "chave-uniterra"

app.config.suppress_callback_exceptions = True

# Memória Interna Rápida
cache = {
    'df': pd.DataFrame(),
    'df_entrada': pd.DataFrame(),
    'categorias': [],
    'maquinas': [],
    'meses': [],
    'N': 0,
    'opcoes_drop': []
}

def tratar_numeros_br(series):
    """
    Escudo de Números: Limpa qualquer bagunça de digitação do Excel/Google Sheets.
    """
    if pd.api.types.is_numeric_dtype(series):
        return series
    
    def limpar(x):
        if pd.isna(x): return np.nan
        x = str(x).strip()
        if x == '': return np.nan
        
        if ',' in x:
            return x.replace('.', '').replace(',', '.')
        elif x.count('.') >= 1:
            partes = x.split('.')
            if len(partes[-1]) == 3:
                return x.replace('.', '')
        return x

    return pd.to_numeric(series.apply(limpar), errors='coerce')

def baixar_e_processar_dados():
    """Baixa os dados das DUAS planilhas em segundo plano e prepara para os gráficos."""
    try:
        # --- PLANILHA 1: LANÇAMENTOS / CONSUMO ---
        req = urllib.request.Request(
            LINK_GOOGLE_SHEETS, 
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            conteudo_csv = response.read().decode('utf-8-sig')
        
        df = pd.read_csv(io.StringIO(conteudo_csv))
        if df.empty: return False

        df.columns = [str(c).strip().upper().replace('\ufeff', '').replace('Ï»¿', '') for c in df.columns]
        if len(df.columns) > 0:
            df.rename(columns={df.columns[0]: 'DATA'}, inplace=True)

        df['DATA'] = pd.to_datetime(df['DATA'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['DATA'])
        
        col_km, col_litros, col_mes, col_cat = 'HOR/KM ATUAL', 'QUANT COMB', 'MÊS REF', 'CATEGORIA'

        for col in [col_litros, col_km]:
            if col in df.columns:
                df[col] = tratar_numeros_br(df[col])

        if col_mes in df.columns:
            df[col_mes] = df[col_mes].astype(str)
            df['DATA_REF'] = pd.to_datetime(df[col_mes], errors='coerce')
            df['MES_STR'] = df['DATA_REF'].dt.strftime('%m/%Y') 
            df = df.dropna(subset=['DATA_REF'])

        df = df.sort_values(by=['MAQUINA', 'DATA'])
        
        if col_km in df.columns and col_litros in df.columns:
            df['KM_VALIDO'] = df[col_km].replace(0, np.nan)
            df['REF_ANTERIOR'] = df.groupby('MAQUINA')['KM_VALIDO'].transform(lambda x: x.ffill().shift(1))
            
            # CÁLCULO DO TRABALHO DO MÊS (Horas ou Km)
            df['TRABALHO'] = df['KM_VALIDO'] - df['REF_ANTERIOR']
            df['TRABALHO'] = df['TRABALHO'].apply(lambda x: x if pd.notna(x) and x > 0 else np.nan)
            
            # CÁLCULO DE CONSUMO
            df['CONSUMO'] = df['TRABALHO'] / df[col_litros]
            df['CONSUMO'] = df['CONSUMO'].replace([np.inf, -np.inf], np.nan)
            
            # ESCUDO ANTI-ERROS: Se o consumo deu um valor impossível (ex: > 25 km/L),
            # significa que digitaram o KM errado. Anulamos o Consumo E o Trabalho.
            erros_digitacao = (df['CONSUMO'] <= 0) | (df['CONSUMO'] > 25)
            df.loc[erros_digitacao, 'CONSUMO'] = np.nan
            df.loc[erros_digitacao, 'TRABALHO'] = np.nan
            
            # Escudo extra de segurança: Nenhuma máquina trabalha mais de 15.000 km/horas entre um abastecimento e outro
            df.loc[df['TRABALHO'] > 15000, 'TRABALHO'] = np.nan
        
        if col_cat in df.columns:
            df[col_cat] = df[col_cat].fillna('Outros').astype(str)
        else:
            df[col_cat] = 'Geral'

        cache['df'] = df
        cache['categorias'] = sorted(df[col_cat].unique()) if col_cat in df.columns else []
        cache['maquinas'] = sorted(df['MAQUINA'].unique()) if 'MAQUINA' in df.columns else []
        
        if 'MAQUINA' in df.columns and col_cat in df.columns:
            maq_df = df[['MAQUINA', col_cat]].drop_duplicates().sort_values(by=[col_cat, 'MAQUINA'])
            cache['opcoes_drop'] = [{'label': f"{row[col_cat]} - {row['MAQUINA']}", 'value': row['MAQUINA']} for idx, row in maq_df.iterrows()]
        
        if 'DATA_REF' in df.columns:
            cache['meses'] = sorted(df['DATA_REF'].unique())
            cache['N'] = len(cache['meses'])

        # --- PLANILHA 2: ENTRADA DE DIESEL ---
        req2 = urllib.request.Request(
            LINK_ENTRADA_DIESEL, 
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        )
        with urllib.request.urlopen(req2, timeout=10) as response2:
            conteudo_entrada = response2.read().decode('utf-8-sig')
            
        df_ent = pd.read_csv(io.StringIO(conteudo_entrada))
        df_ent.columns = [str(c).strip().upper().replace('\ufeff', '').replace('Ï»¿', '') for c in df_ent.columns]
        
        if 'LITROS' in df_ent.columns:
            df_ent['LITROS'] = tratar_numeros_br(df_ent['LITROS'])
            
        col_preco = None
        for col in df_ent.columns:
            if 'R$' in col or 'PRECO' in col or 'PREÇO' in col or 'VALOR' in col:
                col_preco = col
                break
                
        if col_preco:
            df_ent['PRECO'] = df_ent[col_preco].astype(str).str.replace('R$', '', regex=False).str.replace('"', '', regex=False)
            df_ent['PRECO'] = tratar_numeros_br(df_ent['PRECO'])
        else:
            df_ent['PRECO'] = np.nan

        nome_col_data_ent = 'DATA ABAST' if 'DATA ABAST' in df_ent.columns else df_ent.columns[0]
        df_ent['DATA'] = pd.to_datetime(df_ent[nome_col_data_ent], dayfirst=True, errors='coerce')
        df_ent = df_ent.dropna(subset=['DATA'])
        
        df_ent['DATA_REF'] = pd.to_datetime(df_ent['DATA'].dt.strftime('%Y-%m-01'))
        df_ent['MES_STR'] = df_ent['DATA_REF'].dt.strftime('%m/%Y')
        
        df_ent = df_ent.sort_values('DATA')

        cache['df_entrada'] = df_ent

        return True
    except Exception as e:
        print(f"Erro no download (Timeout evitado): {e}")
        return False

# =========================================================================
# LAYOUT INICIAL INSTANTÂNEO
# =========================================================================
app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'backgroundColor': '#ECEFF1', 'padding': '20px', 'margin': '0', 'minHeight': '100vh'}, children=[
    dcc.Location(id='url-gatilho'), 
    
    html.Div(style={'backgroundColor': '#2C3E50', 'color': 'white', 'padding': '15px', 'borderRadius': '8px', 'marginBottom': '20px', 'textAlign': 'center', 'boxShadow': '0 4px 6px rgba(0,0,0,0.2)'}, children=[
        html.H1("Controle de Combustível - Uniterra", style={'margin': '0'})
    ]),
    
    dcc.Loading(
        id="loading-painel",
        type="circle",
        color="#3498DB",
        style={'marginTop': '100px'},
        children=html.Div(id='painel-pronto-container') 
    )
])

# =========================================================================
# MOTOR ASSÍNCRONO (Gera a tela completa)
# =========================================================================
@app.callback(
    Output('painel-pronto-container', 'children'),
    Input('url-gatilho', 'pathname')
)
def construir_tela_completa(pathname):
    sucesso = baixar_e_processar_dados()
    
    if not sucesso and cache['df'].empty:
        return html.Div([
            html.H2("⚠️ Lentidão no Google Sheets", style={'color': '#E74C3C'}),
            html.P("O Google demorou para enviar a planilha e interrompemos a conexão para não travar o seu celular."),
            html.P("Por favor, atualize a página para tentar novamente.")
        ], style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': '40px', 'borderRadius': '8px'})

    lista_categorias = cache['categorias']
    lista_maquinas = cache['maquinas']
    opcoes_drop_maquina = cache['opcoes_drop']
    opcoes_drop_cat = [{'label': cat, 'value': cat} for cat in lista_categorias]
    N = cache['N']
    estilo_caixa = {'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '8px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)', 'marginBottom': '30px'}

    return html.Div([
        # 1. VISÃO GERAL
        html.Div(style=estilo_caixa, children=[
            html.H2("1. Visão Geral da Empresa", style={'marginTop': '0', 'color': '#34495E'}),
            html.Div(style={'backgroundColor': '#F8F9F9', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px'}, children=[
                html.Strong("Filtro de Categoria:"),
                html.Div(style={'marginBottom': '10px', 'marginTop': '5px'}, children=[
                    html.Button('Selecionar Todas', id='btn-todas-cat', n_clicks=0, style={'marginRight': '10px', 'cursor': 'pointer', 'backgroundColor': '#3498DB', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'}),
                    html.Button('Limpar Seleção', id='btn-nenhuma-cat', n_clicks=0, style={'cursor': 'pointer', 'backgroundColor': '#95A5A6', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'})
                ]),
                dcc.Checklist(id='check-categoria', options=[{'label': cat, 'value': cat} for cat in lista_categorias], value=lista_categorias, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, style={'display': 'flex', 'flexWrap': 'wrap', 'marginBottom': '20px'}),

                html.Strong("Filtro de Máquinas:"),
                html.Div(style={'marginBottom': '10px', 'marginTop': '5px'}, children=[
                    html.Button('Selecionar Todas', id='btn-todas', n_clicks=0, style={'marginRight': '10px', 'cursor': 'pointer', 'backgroundColor': '#3498DB', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'}),
                    html.Button('Limpar Seleção', id='btn-nenhuma', n_clicks=0, style={'cursor': 'pointer', 'backgroundColor': '#95A5A6', 'color': 'white', 'border': 'none', 'padding': '5px 10px', 'borderRadius': '3px'})
                ]),
                dcc.Checklist(id='check-maquinas', options=[{'label': maq, 'value': maq} for maq in lista_maquinas], value=lista_maquinas, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, style={'display': 'flex', 'flexWrap': 'wrap', 'marginBottom': '15px'}),
                
                html.Strong("Período de Análise:"),
                dcc.RadioItems(id='radio-tempo', options=[{'label': 'Este mês', 'value': 1}, {'label': 'Últimos 3 meses', 'value': 3}, {'label': 'Últimos 6 meses', 'value': 6}, {'label': 'Últimos 12 meses', 'value': 12}, {'label': 'Total disponível', 'value': N}], value=N, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'})
            ]),
            dcc.Graph(id='grafico-geral'),
            html.Div(id='tabela-geral-container', style={'marginTop': '20px'})
        ]),
        
        # 2. ANÁLISE POR CATEGORIA 
        html.Div(style=estilo_caixa, children=[
            html.H2("2. Análise Detalhada por Categoria", style={'marginTop': '0', 'color': '#34495E'}),
            html.Div(style={'display': 'flex', 'gap': '20px', 'backgroundColor': '#F8F9F9', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px', 'flexWrap': 'wrap'}, children=[
                html.Div(style={'flex': '1', 'minWidth': '300px'}, children=[
                    html.Strong("Selecione a Categoria:"),
                    dcc.Dropdown(id='drop-categoria-analise', options=opcoes_drop_cat, value=opcoes_drop_cat[0]['value'] if opcoes_drop_cat else None, clearable=False, style={'marginTop': '5px'})
                ]),
                html.Div(style={'flex': '2', 'minWidth': '300px'}, children=[
                    html.Strong("Período:"),
                    dcc.RadioItems(id='radio-tempo-cat', options=[{'label': 'Este mês', 'value': 1}, {'label': 'Últimos 3 meses', 'value': 3}, {'label': 'Últimos 6 meses', 'value': 6}, {'label': 'Últimos 12 meses', 'value': 12}, {'label': 'Total disponível', 'value': N}], value=N, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, style={'marginTop': '10px'})
                ])
            ]),
            dcc.Graph(id='grafico-detalhe-cat'),
            html.Div(id='tabela-detalhe-cat-container', style={'marginTop': '20px'})
        ]),

        # 3. ANÁLISE POR MÁQUINA
        html.Div(style=estilo_caixa, children=[
            html.H2("3. Análise Detalhada por Máquina", style={'marginTop': '0', 'color': '#34495E'}),
            html.Div(style={'display': 'flex', 'gap': '20px', 'backgroundColor': '#F8F9F9', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px', 'flexWrap': 'wrap'}, children=[
                html.Div(style={'flex': '1', 'minWidth': '300px'}, children=[
                    html.Strong("Selecione a Máquina:"),
                    dcc.Dropdown(id='drop-maquina', options=opcoes_drop_maquina, value=opcoes_drop_maquina[0]['value'] if opcoes_drop_maquina else None, clearable=False, style={'marginTop': '5px'})
                ]),
                html.Div(style={'flex': '2', 'minWidth': '300px'}, children=[
                    html.Strong("Período:"),
                    dcc.RadioItems(id='radio-tempo-maq', options=[{'label': 'Este mês', 'value': 1}, {'label': 'Últimos 3 meses', 'value': 3}, {'label': 'Últimos 6 meses', 'value': 6}, {'label': 'Últimos 12 meses', 'value': 12}, {'label': 'Total disponível', 'value': N}], value=N, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, style={'marginTop': '10px'})
                ])
            ]),
            dcc.Graph(id='grafico-detalhe'),
            html.Div(id='tabela-detalhe-container', style={'marginTop': '20px'})
        ]),
        
        # 4. BALANÇO ENTRADA VS CONSUMO 
        html.Div(style=estilo_caixa, children=[
            html.H2("4. Balanço Global: Compras vs Consumo na Frota", style={'marginTop': '0', 'color': '#34495E'}),
            html.Div(style={'backgroundColor': '#F8F9F9', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px'}, children=[
                html.Strong("Período:"),
                dcc.RadioItems(id='radio-tempo-balanco', options=[{'label': 'Este mês', 'value': 1}, {'label': 'Últimos 3 meses', 'value': 3}, {'label': 'Últimos 6 meses', 'value': 6}, {'label': 'Últimos 12 meses', 'value': 12}, {'label': 'Total disponível', 'value': N}], value=N, inline=True, inputStyle={'marginRight': '5px', 'marginLeft': '15px'}, style={'marginTop': '5px'})
            ]),
            dcc.Graph(id='grafico-balanco'),
            html.Div(id='tabela-balanco-container', style={'marginTop': '20px'})
        ])
    ])

# =========================================================================
# LÓGICA DE INTERAÇÃO DOS GRÁFICOS
# =========================================================================
@app.callback(
    Output('check-categoria', 'value'),
    Input('btn-todas-cat', 'n_clicks'), Input('btn-nenhuma-cat', 'n_clicks'),
    prevent_initial_call=True
)
def update_cat_checks(btn_t, btn_n):
    ctx = callback_context
    if ctx.triggered[0]['prop_id'].split('.')[0] == 'btn-todas-cat': return cache['categorias']
    return []

@app.callback(
    Output('check-maquinas', 'options'), Output('check-maquinas', 'value'),
    Input('check-categoria', 'value'), Input('btn-todas', 'n_clicks'), Input('btn-nenhuma', 'n_clicks'),
    State('check-maquinas', 'value'),
    prevent_initial_call=False
)
def update_maq_checks(cat_selec, btn_t, btn_n, maq_atuais):
    ctx = callback_context
    id_acionado = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None
    df = cache['df']
    if df.empty: return [], []
    
    disponiveis = sorted(df[df['CATEGORIA'].isin(cat_selec or [])]['MAQUINA'].unique())
    opcoes = [{'label': maq, 'value': maq} for maq in disponiveis]
    
    if id_acionado == 'btn-todas': return opcoes, disponiveis
    if id_acionado == 'btn-nenhuma': return opcoes, []
    
    validas = [m for m in (maq_atuais or []) if m in disponiveis]
    return opcoes, (validas if validas or not disponiveis else disponiveis)

# 1. VISÃO GERAL CALLBACK
@app.callback(
    Output('grafico-geral', 'figure'), Output('tabela-geral-container', 'children'),
    Input('check-maquinas', 'value'), Input('radio-tempo', 'value')
)
def update_geral(maquinas, meses_n):
    df = cache['df']
    vazio = html.Div()
    if not maquinas or df.empty: return go.Figure(), vazio
    
    df_f = df[df['MAQUINA'].isin(maquinas)]
    meses_v = cache['meses'][-meses_n:] if meses_n > 0 else []
    df_f = df_f[df_f['DATA_REF'].isin(meses_v)]
    if df_f.empty: return go.Figure(), vazio

    lista_meses_texto = sorted(df_f['MES_STR'].unique(), key=lambda x: pd.to_datetime(x, format='%m/%Y'))

    fig = make_subplots(rows=2, cols=1, vertical_spacing=0.1, specs=[[{"type": "xy"}], [{"type": "domain"}]], subplot_titles=("Evolução Mensal (Litros)", "Proporção de Consumo por Máquina"))
    
    df_m = df_f.groupby('MES_STR')['QUANT COMB'].sum().reset_index()
    soma_str = f"{df_m['QUANT COMB'].sum():,.0f} Litros".replace(',', '.')
    
    fig.add_trace(go.Bar(x=df_m['MES_STR'], y=df_m['QUANT COMB'], text=df_m['QUANT COMB'].round(0), textposition='auto', marker_color='#27AE60', name='Total', showlegend=False), row=1, col=1)

    df_res = df_f.groupby('MAQUINA').agg({'QUANT COMB':'sum', 'CONSUMO':'mean', 'CATEGORIA':'first'}).reset_index().sort_values(by='QUANT COMB', ascending=False)
    df_tree = df_res[df_res['QUANT COMB'] > 0]
    
    if not df_tree.empty:
        cats = sorted(df_tree['CATEGORIA'].unique())
        mapa = {c: px.colors.qualitative.Plotly[i % 10] for i, c in enumerate(cats)}
        df_tree['TEXTO_HOVER'] = "Categoria: " + df_tree['CATEGORIA']
        fig.add_trace(go.Treemap(labels=df_tree['MAQUINA'], parents=[""]*len(df_tree), values=df_tree['QUANT COMB'], text=df_tree['TEXTO_HOVER'], textinfo="label+value+percent root", marker=dict(colors=df_tree['CATEGORIA'].map(mapa)), hovertemplate="<b>%{label}</b><br>%{text}<br>Consumo Total: %{value:,.0f} Litros<br>Representa: %{percentRoot:.1%} da seleção<extra></extra>"), row=2, col=1)
        for c in cats: fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', marker=dict(color=mapa[c], symbol='square'), name=c), row=1, col=1)

    fig.update_layout(title=f"Soma do Período Filtrado: <b>{soma_str}</b>", template="plotly_white", height=800, margin=dict(t=50), font=dict(family="Arial, sans-serif"))
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=1, col=1)
    
    dt = pd.DataFrame({'Máquina': df_res['MAQUINA'], 'Categoria': df_res['CATEGORIA'], 'Total (L)': df_res['QUANT COMB'].map('{:,.0f}'.format), 'Consumo': df_res['CONSUMO'].map('{:.2f}'.format)})
    tab = dash_table.DataTable(data=dt.to_dict('records'), columns=[{'name': i, 'id': i} for i in dt.columns], style_table={'overflowX': 'auto'}, style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'}, style_header={'backgroundColor': '#2C3E50', 'color': 'white', 'fontWeight': 'bold'}, style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}], page_size=10)
    
    # === TABELA: 10 ÚLTIMOS ABASTECIMENTOS ===
    df_ultimos = df_f.sort_values(by='DATA', ascending=False).head(10).copy()
    
    col_motorista = None
    for col in ['MOTORISTA', 'OPERADOR', 'FUNCIONÁRIO', 'FUNCIONARIO']:
        if col in df_ultimos.columns:
            col_motorista = col
            break
            
    df_ult10 = pd.DataFrame()
    df_ult10['Data'] = df_ultimos['DATA'].dt.strftime('%d/%m/%Y')
    df_ult10['Máquina'] = df_ultimos['MAQUINA']
    df_ult10['Categoria'] = df_ultimos['CATEGORIA']
    df_ult10['Quantidade'] = df_ultimos['QUANT COMB'].apply(lambda x: f"{x:,.0f} L".replace(',', '.'))
    
    if col_motorista:
        df_ult10['Motorista'] = df_ultimos[col_motorista].fillna('-')
    else:
        df_ult10['Motorista'] = '-'
        
    df_ult10['Média Consumo'] = df_ultimos['CONSUMO'].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "S/D")
    
    tab_ultimos = dash_table.DataTable(
        data=df_ult10.to_dict('records'), 
        columns=[{'name': i, 'id': i} for i in df_ult10.columns], 
        style_table={'overflowX': 'auto'}, 
        style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'}, 
        style_header={'backgroundColor': '#34495E', 'color': 'white', 'fontWeight': 'bold'}, 
        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}]
    )
    
    return fig, html.Div([
        html.H3("Matriz de Dados do Período", style={'textAlign': 'center', 'color': '#34495E'}), 
        tab,
        html.H3("Últimos 10 Abastecimentos da Frota", style={'textAlign': 'center', 'color': '#34495E', 'marginTop': '40px'}),
        tab_ultimos
    ])

# 2. CATEGORIA CALLBACK (AGORA COM 6 GRÁFICOS INCLUINDO HORAS/KM TRABALHADAS)
@app.callback(
    Output('grafico-detalhe-cat', 'figure'), Output('tabela-detalhe-cat-container', 'children'),
    Input('drop-categoria-analise', 'value'), Input('radio-tempo-cat', 'value')
)
def update_detalhe_cat(cat, meses_n):
    df = cache['df']
    vazio = html.Div()
    if not cat or df.empty: return go.Figure(), vazio
    
    df_c = df[df['CATEGORIA'] == cat]
    meses_v = cache['meses'][-meses_n:] if meses_n > 0 else []
    df_c = df_c[df_c['DATA_REF'].isin(meses_v)]
    if df_c.empty: return go.Figure(), vazio

    # Agrupamentos
    df_g = df_c.groupby('MES_STR').agg({'CONSUMO': 'mean', 'QUANT COMB': 'sum', 'TRABALHO': 'sum'}).reset_index()
    lista_meses_texto = sorted(df_g['MES_STR'].unique(), key=lambda x: pd.to_datetime(x, format='%m/%Y'))
    
    df_g_maq = df_c.groupby(['MES_STR', 'MAQUINA']).agg({'CONSUMO': 'mean', 'QUANT COMB': 'sum', 'TRABALHO': 'sum'}).reset_index()
    df_g_maq['DATA_SORT'] = pd.to_datetime(df_g_maq['MES_STR'], format='%m/%Y')
    df_g_maq = df_g_maq.sort_values(['DATA_SORT', 'MAQUINA'])

    # Filtros para não desenhar espaços vazios
    df_g_maq_c = df_g_maq[df_g_maq['CONSUMO'] > 0].copy()
    df_g_maq_t = df_g_maq[df_g_maq['TRABALHO'] > 0].copy()
    df_g_maq_q = df_g_maq[df_g_maq['QUANT COMB'] > 0].copy()

    # Médias Globais
    m_geral_consumo = df_c['CONSUMO'].mean()
    m_geral_litros = df_g['QUANT COMB'].mean()
    m_geral_trabalho = df_g['TRABALHO'].mean()
    
    if pd.isna(m_geral_consumo): m_geral_consumo = 0 
    if pd.isna(m_geral_litros): m_geral_litros = 0
    if pd.isna(m_geral_trabalho): m_geral_trabalho = 0

    # Layout de 6 Gráficos com simetria e espaçamento de respiro
    fig = make_subplots(
        rows=6, cols=1, 
        vertical_spacing=0.10, 
        row_heights=[0.12, 0.21, 0.21, 0.12, 0.12, 0.22], 
        subplot_titles=(
            f"1. Média Geral de Consumo da Categoria ({cat})", 
            f"2. Média Mensal de Consumo por Máquina",
            f"3. Horas/Km Trabalhadas Mensal por Máquina",
            f"4. Total de Horas/Km Trabalhadas da Categoria ({cat})",
            f"5. Volume Total de Combustível Gasto ({cat})",
            f"6. Volume Mensal Gasto por Máquina"
        )
    )
    
    maquinas_unicas = df_g_maq['MAQUINA'].unique()
    cores = px.colors.qualitative.Plotly
    mapa_cores = {m: cores[i % len(cores)] for i, m in enumerate(maquinas_unicas)}
    
    # --- ROW 1: Média Consumo Categoria ---
    fig.add_trace(go.Bar(
        x=df_g['MES_STR'], y=df_g['CONSUMO'], 
        text=df_g['CONSUMO'].round(2), textposition='auto', 
        marker_color='#8E44AD', name='Consumo Categoria', showlegend=False
    ), row=1, col=1)
    
    if m_geral_consumo > 0:
        fig.add_hline(y=m_geral_consumo, line_dash="dash", line_color="#E74C3C", annotation_text=f"Média: {m_geral_consumo:.1f}", annotation_position="top right", row=1, col=1)
        
    # --- ROW 2: Média Consumo Máquina ---
    cores_maq_c = [mapa_cores[m] for m in df_g_maq_c['MAQUINA']]
    fig.add_trace(go.Bar(
        x=[df_g_maq_c['MES_STR'], df_g_maq_c['MAQUINA']], y=df_g_maq_c['CONSUMO'],
        text=df_g_maq_c['CONSUMO'].apply(lambda x: f"{x:.2f}".replace('.', ',')),
        textposition='outside', textangle=-90, marker_color=cores_maq_c,
        name='Consumo Máquina', showlegend=False, cliponaxis=False        
    ), row=2, col=1)

    if m_geral_consumo > 0:
        fig.add_hline(y=m_geral_consumo, line_dash="dash", line_color="#E74C3C", annotation_text=f"Média Geral", annotation_position="top right", row=2, col=1)

    # --- ROW 3: Trabalho Máquina (Novo Gráfico) ---
    cores_maq_t = [mapa_cores[m] for m in df_g_maq_t['MAQUINA']]
    fig.add_trace(go.Bar(
        x=[df_g_maq_t['MES_STR'], df_g_maq_t['MAQUINA']], y=df_g_maq_t['TRABALHO'],
        text=df_g_maq_t['TRABALHO'].apply(lambda x: f"{x:,.0f}".replace(',', '.')),
        textposition='outside', textangle=-90, marker_color=cores_maq_t,
        name='Trabalho Máquina', showlegend=False, cliponaxis=False        
    ), row=3, col=1)

    m_trab_maq = df_g_maq_t['TRABALHO'].mean()
    if m_trab_maq > 0:
        fig.add_hline(y=m_trab_maq, line_dash="dash", line_color="#3498DB", annotation_text=f"Média por Máquina", annotation_position="top right", row=3, col=1)

    # --- ROW 4: Trabalho Categoria (Novo Gráfico) ---
    fig.add_trace(go.Bar(
        x=df_g['MES_STR'], y=df_g['TRABALHO'], 
        text=df_g['TRABALHO'].round(0), textposition='auto', 
        marker_color='#2980B9', name='Trabalho Categoria', showlegend=False
    ), row=4, col=1)

    if m_geral_trabalho > 0:
        fig.add_hline(y=m_geral_trabalho, line_dash="dash", line_color="#3498DB", annotation_text=f"Média Mensal", annotation_position="top right", row=4, col=1)

    # --- ROW 5: Litros Categoria ---
    fig.add_trace(go.Bar(
        x=df_g['MES_STR'], y=df_g['QUANT COMB'], 
        text=df_g['QUANT COMB'].round(0), textposition='auto', 
        marker_color='#F39C12', name='Litros Categoria', showlegend=False
    ), row=5, col=1)

    if m_geral_litros > 0:
        fig.add_hline(y=m_geral_litros, line_dash="dash", line_color="#27AE60", annotation_text=f"Média do Período", annotation_position="top right", row=5, col=1)

    # --- ROW 6: Litros Máquina ---
    cores_maq_q = [mapa_cores[m] for m in df_g_maq_q['MAQUINA']]
    fig.add_trace(go.Bar(
        x=[df_g_maq_q['MES_STR'], df_g_maq_q['MAQUINA']], y=df_g_maq_q['QUANT COMB'],
        text=df_g_maq_q['QUANT COMB'].apply(lambda x: f"{x:,.0f}".replace(',', '.')),
        textposition='outside', textangle=-90, marker_color=cores_maq_q,
        name='Litros Máquina', showlegend=False, cliponaxis=False
    ), row=6, col=1)
    
    m_litros_maq = df_g_maq_q['QUANT COMB'].mean()
    if m_litros_maq > 0:
        fig.add_hline(y=m_litros_maq, line_dash="dash", line_color="#27AE60", annotation_text=f"Média por Máquina", annotation_position="top right", row=6, col=1)

    # Legenda Fantasma
    for maq in maquinas_unicas:
        fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', marker=dict(color=mapa_cores[maq], symbol='square', size=10), name=maq), row=1, col=1)

    # --- CONFIGURAÇÕES GERAIS ---
    fig.update_layout(
        title=f"Visão Completa de Categoria vs Máquinas: <b>{cat}</b>", 
        template="plotly_white", 
        height=2400, # Aumentado para 2400px para acomodar os 6 gráficos sem embolar
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(family="Arial, sans-serif"),
        barmode='group'
    )
    
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=1, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=4, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=5, col=1)
    
    fig.update_xaxes(tickangle=-90, row=2, col=1)
    fig.update_xaxes(tickangle=-90, row=3, col=1)
    fig.update_xaxes(tickangle=-90, row=6, col=1)

    # Margem extra no teto das barras agrupadas
    if not df_g_maq_c.empty: fig.update_yaxes(range=[0, df_g_maq_c['CONSUMO'].max() * 1.4], row=2, col=1)
    if not df_g_maq_t.empty: fig.update_yaxes(range=[0, df_g_maq_t['TRABALHO'].max() * 1.4], row=3, col=1)
    if not df_g_maq_q.empty: fig.update_yaxes(range=[0, df_g_maq_q['QUANT COMB'].max() * 1.4], row=6, col=1)

    # --- TABELA DE HISTÓRICO ---
    df_h = df_c[['DATA', 'MAQUINA', 'QUANT COMB', 'CONSUMO']].sort_values(by='DATA', ascending=False).reset_index(drop=True)
    df_h['DATA'] = df_h['DATA'].dt.strftime('%d/%m/%Y')
    df_h['QUANT COMB'] = df_h['QUANT COMB'].apply(lambda x: f"{x:,.0f} L".replace(',', '.'))
    df_h['CONSUMO'] = df_h['CONSUMO'].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "S/D")
    df_h.rename(columns={'DATA': 'Data', 'MAQUINA': 'Máquina', 'QUANT COMB': 'Qtd. Abastecida', 'CONSUMO': 'Consumo Reg.'}, inplace=True)
    
    tab = dash_table.DataTable(data=df_h.to_dict('records'), columns=[{'name': i, 'id': i} for i in df_h.columns], style_table={'overflowX': 'auto'}, style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'}, style_header={'backgroundColor': '#2C3E50', 'color': 'white', 'fontWeight': 'bold'}, style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}], page_size=10)
    
    return fig, html.Div([html.H3(f"Histórico de Abastecimentos - {cat}", style={'textAlign': 'center', 'color': '#34495E'}), tab])

# 3. MÁQUINA CALLBACK (AGORA COM 3 GRÁFICOS INCLUINDO HORAS/KM TRABALHADAS)
@app.callback(
    Output('grafico-detalhe', 'figure'), Output('tabela-detalhe-container', 'children'),
    Input('drop-maquina', 'value'), Input('radio-tempo-maq', 'value')
)
def update_detalhe(maq, meses_n):
    df = cache['df']
    vazio = html.Div()
    if not maq or df.empty: return go.Figure(), vazio
    
    df_m = df[df['MAQUINA'] == maq]
    meses_v = cache['meses'][-meses_n:] if meses_n > 0 else []
    df_m = df_m[df_m['DATA_REF'].isin(meses_v)]
    if df_m.empty: return go.Figure(), vazio

    # Adicionado 'TRABALHO' no agrupamento mensal
    df_g = df_m.groupby('MES_STR').agg({'CONSUMO': 'mean', 'QUANT COMB': 'sum', 'TRABALHO': 'sum'}).reset_index()
    
    lista_meses_texto = sorted(df_g['MES_STR'].unique(), key=lambda x: pd.to_datetime(x, format='%m/%Y'))
    
    m_geral_cons = df_m['CONSUMO'].mean()
    m_geral_trab = df_g['TRABALHO'].mean()
    m_geral_lit = df_g['QUANT COMB'].mean()
    
    if pd.isna(m_geral_cons): m_geral_cons = 0 
    if pd.isna(m_geral_trab): m_geral_trab = 0 
    if pd.isna(m_geral_lit): m_geral_lit = 0 

    fig = make_subplots(
        rows=3, cols=1, 
        vertical_spacing=0.15, 
        subplot_titles=("Média de Consumo Mensal (Km/L ou Horas/L)", "Horas/Km Trabalhadas no Mês", "Total de Combustível Gasto (Litros)")
    )
    
    # Linha 1: Consumo
    fig.add_trace(go.Bar(x=df_g['MES_STR'], y=df_g['CONSUMO'], text=df_g['CONSUMO'].round(2), textposition='auto', marker_color='#2980B9', name='Consumo'), row=1, col=1)
    if m_geral_cons > 0:
        fig.add_hline(y=m_geral_cons, line_dash="dash", line_color="#E74C3C", annotation_text=f"Média do Período: {m_geral_cons:.1f}", annotation_position="top right", row=1, col=1)
        
    # Linha 2: Trabalho
    fig.add_trace(go.Bar(x=df_g['MES_STR'], y=df_g['TRABALHO'], text=df_g['TRABALHO'].round(0), textposition='auto', marker_color='#8E44AD', name='Trabalho'), row=2, col=1)
    if m_geral_trab > 0:
        fig.add_hline(y=m_geral_trab, line_dash="dash", line_color="#8E44AD", annotation_text=f"Média do Período: {m_geral_trab:.0f}", annotation_position="top right", row=2, col=1)

    # Linha 3: Litros
    fig.add_trace(go.Bar(x=df_g['MES_STR'], y=df_g['QUANT COMB'], text=df_g['QUANT COMB'].round(0), textposition='auto', marker_color='#F39C12', name='Litros'), row=3, col=1)
    if m_geral_lit > 0:
        fig.add_hline(y=m_geral_lit, line_dash="dash", line_color="#E74C3C", annotation_text=f"Média do Período: {m_geral_lit:.0f}", annotation_position="top right", row=3, col=1)
    
    fig.update_layout(title=f"Desempenho Detalhado: <b>{maq}</b>", template="plotly_white", height=900, showlegend=False, font=dict(family="Arial, sans-serif"))
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=1, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=2, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=3, col=1)

    df_h = df_m[['DATA', 'QUANT COMB', 'CONSUMO']].sort_values(by='DATA', ascending=False).reset_index(drop=True)
    df_h['DATA'] = df_h['DATA'].dt.strftime('%d/%m/%Y')
    df_h['QUANT COMB'] = df_h['QUANT COMB'].apply(lambda x: f"{x:,.0f} L".replace(',', '.'))
    df_h['CONSUMO'] = df_h['CONSUMO'].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "S/D")
    df_h.rename(columns={'DATA': 'Data do Abastecimento', 'QUANT COMB': 'Qtd. Abastecida', 'CONSUMO': 'Consumo Registrado'}, inplace=True)
    
    tab = dash_table.DataTable(data=df_h.to_dict('records'), columns=[{'name': i, 'id': i} for i in df_h.columns], style_table={'overflowX': 'auto'}, style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'}, style_header={'backgroundColor': '#2C3E50', 'color': 'white', 'fontWeight': 'bold'}, style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}], page_size=10)
    
    return fig, html.Div([html.H3("Histórico de Abastecimentos", style={'textAlign': 'center', 'color': '#34495E'}), tab])

# 4. BALANÇO ENTRADA VS CONSUMO CALLBACK
@app.callback(
    Output('grafico-balanco', 'figure'), Output('tabela-balanco-container', 'children'),
    Input('radio-tempo-balanco', 'value')
)
def update_balanco(meses_n):
    df_consumo = cache['df']
    df_entrada = cache['df_entrada']
    vazio = html.Div()
    
    if df_consumo.empty and df_entrada.empty: 
        return go.Figure(), vazio

    meses_v = cache['meses'][-meses_n:] if meses_n > 0 else []
    
    if not df_consumo.empty:
        df_c_filt = df_consumo[df_consumo['DATA_REF'].isin(meses_v)]
        cons_grp = df_c_filt.groupby('MES_STR')['QUANT COMB'].sum().reset_index()
        cons_grp.rename(columns={'QUANT COMB': 'Consumo'}, inplace=True)
    else:
        df_c_filt = pd.DataFrame()
        cons_grp = pd.DataFrame(columns=['MES_STR', 'Consumo'])

    if not df_entrada.empty:
        df_e_filt = df_entrada[df_entrada['DATA_REF'].isin(meses_v)].copy()
        ent_grp = df_e_filt.groupby('MES_STR').agg(Entrada=('LITROS', 'sum')).reset_index()
    else:
        df_e_filt = pd.DataFrame()
        ent_grp = pd.DataFrame(columns=['MES_STR', 'Entrada'])

    df_bal = pd.merge(ent_grp, cons_grp, on='MES_STR', how='outer')
    df_bal['Entrada'] = df_bal['Entrada'].fillna(0)
    df_bal['Consumo'] = df_bal['Consumo'].fillna(0)
    
    if df_bal.empty: return go.Figure(), vazio
    
    df_bal['DATA_SORT'] = pd.to_datetime(df_bal['MES_STR'], format='%m/%Y')
    df_bal = df_bal.sort_values('DATA_SORT')
    lista_meses_texto = df_bal['MES_STR'].tolist()

    total_entrada = df_bal['Entrada'].sum()
    total_consumo = df_bal['Consumo'].sum()
    saldo_total = total_entrada - total_consumo

    str_entrada = f"{total_entrada:,.0f}".replace(',', '.')
    str_consumo = f"{total_consumo:,.0f}".replace(',', '.')
    str_saldo = f"{saldo_total:,.0f}".replace(',', '.')
    cor_saldo = '#27AE60' if saldo_total >= 0 else '#E74C3C' 

    titulo_barras = (
        f"Comparativo: Volume Comprado vs Consumo da Frota (Por Mês)<br>"
        f"<sup><b>Total Comprado:</b> <span style='color:#27AE60;'>{str_entrada} L</span> &nbsp;|&nbsp; "
        f"<b>Total Consumido:</b> <span style='color:#E74C3C;'>{str_consumo} L</span> &nbsp;|&nbsp; "
        f"<b>Saldo:</b> <span style='color:{cor_saldo};'>{str_saldo} L</span></sup>"
    )

    fig = make_subplots(
        rows=2, cols=1, 
        vertical_spacing=0.15,
        row_heights=[0.6, 0.4], 
        subplot_titles=(titulo_barras, "Preço Médio do Diesel (R$/L) - Compras Exatas")
    )
    
    fig.add_trace(go.Bar(x=df_bal['MES_STR'], y=df_bal['Entrada'], name='Entrada (L)', marker_color='#27AE60', text=df_bal['Entrada'].round(0), textposition='auto'), row=1, col=1)
    fig.add_trace(go.Bar(x=df_bal['MES_STR'], y=df_bal['Consumo'], name='Consumo (L)', marker_color='#E74C3C', text=df_bal['Consumo'].round(0), textposition='auto'), row=1, col=1)
    
    media_entrada = df_bal['Entrada'].mean()
    media_consumo = df_bal['Consumo'].mean()
    
    if media_entrada > 0:
        fig.add_hline(y=media_entrada, line_dash="dash", line_color="#27AE60", annotation_text="Média do Período", annotation_position="top left", row=1, col=1)
        
    if media_consumo > 0:
        fig.add_hline(y=media_consumo, line_dash="dash", line_color="#E74C3C", annotation_text="Média do Período", annotation_position="top right", row=1, col=1)

    if not df_e_filt.empty and 'PRECO' in df_e_filt.columns:
        df_preco = df_e_filt.dropna(subset=['DATA', 'PRECO']).groupby('DATA').agg({'PRECO': 'mean'}).reset_index().sort_values('DATA')
        df_preco['DATA_FORMATADA'] = df_preco['DATA'].dt.strftime('%d/%m/%Y')
        
        fig.add_trace(go.Scatter(
            x=df_preco['DATA'], 
            y=df_preco['PRECO'], 
            mode='lines+markers+text',
            name='Preço R$/L',
            text=df_preco['PRECO'].apply(lambda x: f"R$ {x:.2f}".replace('.', ',')),
            textposition='top center',
            hovertemplate="Data: %{customdata}<br>Preço: R$ %{y:.2f}<extra></extra>",
            customdata=df_preco['DATA_FORMATADA'],
            marker=dict(color='#2980B9', size=8),
            line=dict(color='#3498DB', width=2),
            textfont=dict(size=10, color='#2C3E50', weight='bold'),
            cliponaxis=False
        ), row=2, col=1)

        if not df_preco.empty:
            max_preco = df_preco['PRECO'].max()
            min_preco = df_preco['PRECO'].min()
            margem = (max_preco - min_preco) * 0.15 if max_preco != min_preco else max_preco * 0.1
            fig.update_yaxes(range=[min_preco - margem, max_preco + margem], row=2, col=1)

    fig.update_layout(
        barmode='group', 
        template="plotly_white", 
        height=800, 
        font=dict(family="Arial, sans-serif"), 
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=lista_meses_texto, row=1, col=1)
    
    fig.update_xaxes(title_text="Data da Compra", tickformat="%d/%m/%Y", tickangle=-45, row=2, col=1)
    
    df_bal['Saldo'] = df_bal['Entrada'] - df_bal['Consumo']
    df_bal_reverso = df_bal.sort_values('DATA_SORT', ascending=False)
    
    df_table_bal = pd.DataFrame({
        'Mês Ref.': df_bal_reverso['MES_STR'],
        'Entrada na Base (L)': df_bal_reverso['Entrada'].apply(lambda x: f"{x:,.0f} L".replace(',', '.')),
        'Consumo na Frota (L)': df_bal_reverso['Consumo'].apply(lambda x: f"{x:,.0f} L".replace(',', '.')),
        'Saldo do Mês (L)': df_bal_reverso['Saldo'].apply(lambda x: f"{x:,.0f} L".replace(',', '.'))
    })
    
    tab_balanco = dash_table.DataTable(
        data=df_table_bal.to_dict('records'), 
        columns=[{'name': i, 'id': i} for i in df_table_bal.columns], 
        style_table={'overflowX': 'auto'}, 
        style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'}, 
        style_header={'backgroundColor': '#2C3E50', 'color': 'white', 'fontWeight': 'bold'}, 
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}, 
            {'if': {'filter_query': '{Saldo do Mês (L)} contains "-"', 'column_id': 'Saldo do Mês (L)'}, 'color': '#E74C3C', 'fontWeight': 'bold'}
        ], 
        page_size=12
    )

    if not df_e_filt.empty:
        df_hist_preco = df_e_filt.copy().sort_values(by='DATA', ascending=False).reset_index(drop=True)
        df_hist_preco['DATA'] = df_hist_preco['DATA'].dt.strftime('%d/%m/%Y')
        df_hist_preco['LITROS'] = df_hist_preco['LITROS'].apply(lambda x: f"{x:,.0f} L".replace(',', '.') if pd.notna(x) else "-")
        df_hist_preco['PRECO'] = df_hist_preco['PRECO'].apply(lambda x: f"R$ {x:.2f}".replace('.', ',') if pd.notna(x) else "-")
        
        colunas_hist = {'DATA': 'Data da Compra', 'LITROS': 'Volume Comprado', 'PRECO': 'Preço Pago (R$/L)'}
        if 'FORNECEDOR' in df_hist_preco.columns:
            colunas_hist['FORNECEDOR'] = 'Fornecedor'
            df_hist_preco = df_hist_preco[['DATA', 'FORNECEDOR', 'LITROS', 'PRECO']]
        else:
            df_hist_preco = df_hist_preco[['DATA', 'LITROS', 'PRECO']]
            
        df_hist_preco.rename(columns=colunas_hist, inplace=True)
    else:
        df_hist_preco = pd.DataFrame(columns=['Data da Compra', 'Fornecedor', 'Volume Comprado', 'Preço Pago (R$/L)'])

    tab_precos = dash_table.DataTable(
        data=df_hist_preco.to_dict('records'), 
        columns=[{'name': i, 'id': i} for i in df_hist_preco.columns], 
        style_table={'overflowX': 'auto'}, 
        style_cell={'textAlign': 'center', 'padding': '10px', 'fontFamily': 'Arial, sans-serif'}, 
        style_header={'backgroundColor': '#34495E', 'color': 'white', 'fontWeight': 'bold'}, 
        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#F4F6F7'}], 
        page_size=10
    )

    container_tabelas = html.Div([
        html.H3("Balanço Mensal (Entrada vs Saída)", style={'textAlign': 'center', 'color': '#34495E', 'marginTop': '10px'}), 
        tab_balanco,
        html.H3("Histórico Detalhado de Compras", style={'textAlign': 'center', 'color': '#34495E', 'marginTop': '40px'}),
        tab_precos
    ])

    return fig, container_tabelas

if __name__ == "__main__":
    app.run(debug=False)
