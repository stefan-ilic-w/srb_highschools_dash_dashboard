import plotly.express as px
from dash import Dash, dcc, html, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input
from azure_pg1_df_creation import pg1_df_creation

map_df, osn_info_df, dod_info_df, razredi_df = pg1_df_creation()

dash_app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

dash_app.layout = dbc.Container([
    dcc.Location(id='location'),
    # Row for the title
    dbc.Row([
        html.H1("Подаци о средњим школама у Србији", style={"text-align": "center", "margin-bottom": "2%"})
    ]),
    
    # Row for maps and tables
    dbc.Row([
        # Column for the map
        dbc.Col([
            dcc.Graph(
                id="lokacija_skole",
                figure={},
                hoverData={"points": [{"pointIndex": 0}]},
                style = {"width": "100%", "height": "100%", "margin": "0%"}
            )
        ], width = 5, style={"height": "75vh", "margin-top": "0", "margin-left": "0", "margin-right": "1%", "margin-bottom": "1%"}
        ),
        # Column for tables
        dbc.Col([
            html.Table(id="t1", children={},style = {"width": "100%", "height": "100%"}),
        ], width = 2, style = {"height": "75vh", "margin-top": "0", "margin-left": "0%", "margin-right": "1%", "margin-bottom": "1%"}
        ),

        dbc.Col([
            html.Table(id="t2", children={}, style = {"width": "100%", "height": "100%", "margin-right": "0%", "margin-left": "0%"})
        ], width = 2, style = {"height": "75vh", "margin-top": "0", "margin-left": "0%", "margin-right": "1%", "margin-bottom": "1%"}
        ),

        dbc.Col([
            html.Table(id="t3", children={}, style = {"width": "100%", "height": "100vh", "margin": "0%", "margin-right": "0%", "margin-left": "0%"})
        ], width = 2, style = {"height": "75vh", "margin-top": "0", "margin-left": "0", "margin-right": "0", "margin-bottom": "0%"}
        )
    ], className="g-0"),
    
    # Row for the slider
    dbc.Row([
        dbc.Col([
        dcc.Slider(
            min=int(map_df["godina_osnivanja"].min()),
            max=int(map_df["godina_osnivanja"].max()),
            step=1,
            value=int(map_df["godina_osnivanja"].max()),
            marks=None,
            id="godina_osnivanja_slider",
            tooltip={"placement": "bottom", "always_visible": True}
        )
    ], width = 5, align = "center", style = {"height": "5vh"})
    ])
    
], style={"width": "100%", "height": "100vh", "fluid": True})


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@callback(
    Output(component_id="lokacija_skole", component_property="figure"),
    Input(component_id="godina_osnivanja_slider", component_property="value")
)
def update_graph(option_slctd):

    dff = map_df.copy()
    dff = dff[dff["godina_osnivanja"] <= option_slctd]

    # Plotly Express
    px.set_mapbox_access_token(open("mapbox_token").read())
    title = f"Школе основане у Републици Србији до {option_slctd}. године."
    fig = px.scatter_mapbox(dff
                          , lat="latitude"
                          , lon="longitude"
                          , hover_data={"osnivac_kategorija": True, "vrsta_ustanove": True, "naziv_ustanove": True, "latitude": False, "longitude": False}
                          , labels={"osnivac_kategorija": "Врста установе", "vrsta_ustanove": "Тип школе", "naziv_ustanove": "Назив установе"}
                          , zoom = 6
                          , custom_data = ["osnivac_kategorija", "vrsta_ustanove", "naziv_ustanove", "naziv_ustanove_id"]
                          , title = title
                          , mapbox_style="open-street-map"
                          )
    
    fig.update_traces(marker_symbol="circle"
                      , hovertemplate="Врста установе: %{customdata[0]} <br>Тип школе: %{customdata[1]} <br>Назив установе: %{customdata[2]} <br><br>Молим обележите школу за више информација"
                      , selector=dict(type="scattermapbox")
    )
    fig.update_layout(autosize=True
                    , margin = dict(b=5, l=5, t=40, r=5)
                    , mapbox = dict(center=dict(lat=44.03596039242683, lon=20.808460377314884))
                    , title_x = 0.5
    )


    return fig

@callback(
    Output("t1", "children"),
    [Input(component_id="lokacija_skole", component_property="clickData"),
     Input(component_id="godina_osnivanja_slider", component_property="value")]
)
def update_t1(clickData, option_slctd):
    if clickData is None:
        skola_id = 434
    else:
        skola_id = clickData["points"][0]["customdata"][3]
    dff = osn_info_df.copy()
    flt_chk = (dff["godina_osnivanja"] <=option_slctd) & (dff["naziv_ustanove_id"] == skola_id)
    if len(dff.loc[flt_chk])==0:
        option_slctd=2022
        skola_id = 434
    else:
        try:
            clickData["points"][0]["customdata"][3]
            skola_id = clickData["points"][0]["customdata"][3]
        except:
            skola_id = 434
        
    flt_t1 = (dff["naziv_ustanove_id"] == skola_id) & (dff["godina_osnivanja"] <= option_slctd)
    dff = dff.loc[flt_t1]
    dff = dff[["naziv_ustanove", "osnivac_kategorija", "vrsta_ustanove", "okrug", "opstina", "mesto", "ulica", "godina_osnivanja", "maticni_broj", "pib", "dodatno_o_skoli", "podaci_o_razredima"]]
    rename_dict = {"naziv_ustanove": "Назив установе", "osnivac_kategorija": "Врста установе", "vrsta_ustanove": "Тип школе", "okrug": "Округ", "opstina": "Општина", "mesto": "Место", "ulica": "Улица"
                 , "godina_osnivanja": "Година оснивања", "maticni_broj": "Матични број", "pib": "ПИБ", "dodatno_o_skoli": "Додатно о школи", "podaci_o_razredima": "Подаци о разредима"}
    dff = dff.rename(columns = rename_dict)
    dff = dff.T.reset_index()
    dff.columns = ["Врста информације", "Податак"]
    dt_t1 = dash_table.DataTable(data=dff.to_dict("records")
                                ,columns=[
                                    {"name": "Врста информације", "id": "Врста информације"}
                                   ,{"name": "Податак", "id": "Податак"}
                                ]
                                ,style_data_conditional = [
                                     {"if": {"column_id": "Врста информације"}, "width": "45%"}
                                    ,{"if": {"column_id": "Податак"}, "width": "55%"}
                                    ,*[
                                        {
                                            "if": {"column_id": c, "column_type": "numeric"},
                                            "format": {"group": ".", "decimal": ","}
                                        } for c in dff.columns if c != "id"
                                      ]
                                ]
                                ,style_cell={
                                    "whiteSpace": "normal",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0,
                                    "font_size": "12px",
                                }
                                ,style_data={
                                    "font-size": "12px",
                                    "font-family": "Arial, sans-serif"
                                }
                                ,style_header={"font-size": "12px", "font-weight": "bold", "font-family": "Arial, sans-serif", "textAlign": "center"}
                                ,tooltip_data=[
                                    {
                                        column: {"value": str(value), "type": "markdown"}
                                        for column, value in row.items()
                                        } for row in dff.to_dict("records")
                                ]
                                ,tooltip_duration=None

    )

    return dt_t1

@callback(
    Output("t2", "children"),
    [Input(component_id="lokacija_skole", component_property="clickData"),
     Input(component_id="godina_osnivanja_slider", component_property="value")]
)
def update_t2(clickData, option_slctd):
    if clickData is None:
        skola_id = 434
    else:
        skola_id = clickData["points"][0]["customdata"][3]
    dff = dod_info_df.copy()
    flt_chk = (dff["godina_osnivanja"] <=option_slctd) & (dff["naziv_ustanove_id"] == skola_id)
    if len(dff.loc[flt_chk])==0:
        option_slctd=2022
        skola_id = 434
    else:
        try:
            clickData["points"][0]["customdata"][3]
            skola_id = clickData["points"][0]["customdata"][3]
        except:
            skola_id = 434
        
    flt_t2 = (dff["naziv_ustanove_id"] == skola_id) & (dff["godina_osnivanja"] <= option_slctd)
    dff = dff.loc[flt_t2]
    dff = dff[["povrsina_objekta", "broj_ucionica", "povrsina_ucionica", "broj_kuhinja", "povrsina_kuhinja", "broj_biblioteka", "broj_radionica", "povrsina_radionica", "broj_restorana", "broj_kabineta"\
             , "povrsina_kabineta", "broj_laboratorija", "povrsina_laboratorija", "broj_sala", "povrsina_sala"]]
    rename_dict = {"povrsina_objekta": "Површина објекта у м2", "broj_ucionica": "Број учионица", "povrsina_ucionica": "Површина учионица у м2", "broj_kuhinja": "Број кухиња", "povrsina_kuhinja": "Површина кухиња у м2"\
                 , "broj_biblioteka": "Број библиотека", "broj_radionica": "Број радионица", "povrsina_radionica": "Површина радионица у м2", "broj_restorana": "Број ресторана", "broj_kabineta": "Број кабинета"\
                 , "povrsina_kabineta": "Површина кабинета у м2", "broj_laboratorija": "Број лабораторија", "povrsina_laboratorija": "Површина лабораторија у м2", "broj_sala": "Број фискултурних сала", "povrsina_sala": "Површина сала у м2"}
    dff = dff.rename(columns = rename_dict)
    dff = dff.astype(int).map(lambda x: f"{x:,.0f}".replace(",", ".") if isinstance(x, int) else x)
    dff = dff.T.reset_index()
    dff.columns = ["Врста информације", "Податак"]
    dt_t2 = dash_table.DataTable(data=dff.to_dict("records")
                                ,columns=[
                                    {"name": "Врста информације", "id": "Врста информације"}
                                   ,{"name": "Податак", "id": "Податак"}
                                ]
                                ,style_data_conditional = [
                                     {"if": {"column_id": "Врста информације"}, "width": "65%"}
                                    ,{"if": {"column_id": "Податак"}, "width": "35%"}
                                    ,*[
                                        {
                                            "if": {"column_id": c, "column_type": "numeric"},
                                            "format": {"group": ".", "decimal": ","}
                                        } for c in dff.columns if c != "id"
                                      ]
                                ]
                                ,style_cell={
                                    "whiteSpace": "normal",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0,
                                    "font_size": "12px",
                                }
                                ,style_data={
                                    "font-size": "12px",
                                    "font-family": "Arial, sans-serif"
                                }
                                ,style_header={"font-size": "12px", "font-weight": "bold", "font-family": "Arial, sans-serif", "textAlign": "center"}

    )
    
    return dt_t2

@callback(
    Output("t3", "children"),
    [Input("lokacija_skole", "clickData"),
     Input(component_id="godina_osnivanja_slider", component_property="value")]
)
def update_t3(clickData, option_slctd):
    if clickData is None:
        skola_id = 434
    else:
        skola_id = clickData["points"][0]["customdata"][3]
    dff = razredi_df.copy()
    flt_chk = (dff["godina_osnivanja"] <=option_slctd) & (dff["naziv_ustanove_id"] == skola_id)
    if len(dff.loc[flt_chk])==0:
        option_slctd=2022
        skola_id = 434
    else:
        try:
            clickData["points"][0]["customdata"][3]
            skola_id = clickData["points"][0]["customdata"][3]
        except:
            skola_id = 434

    skolska_godina = "2022/2023"
    dff = razredi_df.copy()
    flt_t3 = (dff["naziv_ustanove_id"] == skola_id) & (dff["skolska_godina"] == skolska_godina) & (dff["godina_osnivanja"] <= option_slctd)
    dff = dff.loc[flt_t3]
    dff = dff[["nastavni_program", "odeljenje", "broj_ucenika"]]
    dff = dff.groupby("nastavni_program").agg({"odeljenje": "count", "broj_ucenika": "sum"}).reset_index()
    rename_dict = {"nastavni_program": "Наставни програм", "odeljenje": "Број одељења", "broj_ucenika": "Укупно ученика"}
    dff = dff.rename(columns = rename_dict)
    dt_t3 = dash_table.DataTable(data=dff.to_dict("records")
                                ,columns=[
                                    {"name": "Наставни програм", "id": "Наставни програм"}
                                   ,{"name": "Број одељења", "id": "Број одељења"}
                                   ,{"name": "Укупно ученика", "id":"Укупно ученика"}
                                ]
                                ,style_data={
                                    "font-size": "12px"
                                   ,"font-family": "Arial, sans-serif"
                                }
                                ,style_data_conditional=[
                                    {"if": {"column_id": "Наставни програм"}, "width": "45%"},
                                    {"if": {"column_id": "Број одељења"}, "width": "30%"},
                                    {"if": {"column_id": "Укупно ученика"}, "width": "25%"}
                                ]
                                ,style_header={"font-size": "12px", "font-weight": "bold", "font-family": "Arial, sans-serif", "textAlign": "center"}
                                ,style_table={'height': '520px', 'overflowY': 'auto', "width": "100%", 'border-spacing': '10px'}
                                ,style_cell={
                                    "whiteSpace": "normal",
                                    "overflow": "hidden",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0,
                                    "font_size": "12px",
                                }
                                ,tooltip_data=[
                                    {
                                        column: {"value": str(value), "type": "markdown"}
                                        for column, value in row.items()
                                        } for row in dff.to_dict("records")
                                ]
                                ,tooltip_duration=None
    )

    return dt_t3
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    dash_app.run()
