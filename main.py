import gradio as gr
import datetime
import pandas as pd
import re
import perfonitor.data_treatment as data_treatment
import snowflake_util as sf

LAST_30_DAYS = list(pd.date_range(datetime.date.today() - datetime.timedelta(days=31), datetime.date.today()))
LAST_30_DAYS = [date.date() for date in LAST_30_DAYS]



# 1) Input of the daily alarm report
# 2) Validate that input is expected
# 3) Download needed data from data lake into State components
# 4) Do the processing
# 5) Make Incident List and Trackers Incidents List available for user to download


def validate_daily_alarms_report(selected_date, alarm_file):
    if selected_date in alarm_file.name:
        return True
    else:
        return False

    return


def validate_general_info_file(general_info_path, geography):
    print(("File chosen: " + str(general_info_path.name)))
    try:
        dfs_general_info = pd.read_excel(general_info_path.name, sheet_name=None, engine='openpyxl')

        site_list = dfs_general_info["Site Info"]["Site"].to_list()
        #site_list = pd.read_excel(general_info_path.name, sheet_name='Site Info', engine='openpyxl')["Site"].to_list()

        # df_general_info_calc = pd.read_excel(general_info_path.name, sheet_name='Site Info', index_col=0, engine="openpyxl")
        # add a pre_selection
        pre_selection = site_list

        return site_list, pre_selection, dfs_general_info

    except FileNotFoundError:
        return False, False

    return False, False


def query_single_day_irradiance(selected_date):

    query = ''' 
    USE SCHEMA CURATED; 
    create temporary table temp_measurement_dim2 as 
    select 
        S.SITE_KEY, 
        D.DEVICE_KEY, 
        M.MEASUREMENT_KEY, 
        S.SITE_ID, 
        D.DEVICE_ID, 
        M.MEASUREMENT_ID, 
        S.SITE_NAME, 
        M.MEASUREMENT_NAME, 
        M.MEASUREMENT_APCODE, 
        S.COUNTRY, 
        S.TYPE_OF_PROJECT, 
        M.ENGINEERING_UNIT 
    from 
        CURATED.REP_DIM_SITES S 
        INNER JOIN CURATED.REP_DIM_DEVICES D on S.SITE_KEY = D.SITE_FKEY 
        INNER JOIN CURATED.DIM_MEASUREMENTS M on M.DEVICE_FKEY = D.DEVICE_KEY 
    where 
        SITE_NAME like '%Vendimia%' 
        and SITE_KEY not in (462) 
        and MEASUREMENT_APCODE in ( 
            'EnergyExported60m', 
            'EnergyForecastSessionTwo60m', 
            'X1scadaSessionTwo','RelativeHumidity','AmbientAirTemperature' 
        ) 
    order by 
        SITE_NAME, 
        MEASUREMENT_APCODE; 
    -- GET MEASUREMENT DIMENSIONS 
    select * from temp_measurement_dim2; 
    -- GET MEASUREMENTS 
    select 
        D.SITE_ID,D.MEASUREMENT_KEY, LOCAL_DATE, LOCAL_TIME, MDATE, MTIME, MEASUREMENT_VALUE_NUMERIC 
    from 
        temp_measurement_dim2 D 
        inner join RAW.FACT_SITE_MEASUREMENTS SM on SM.SITE_KEY = D.SITE_KEY 
        and SM.DEVICE_KEY = D.DEVICE_KEY 
        and SM.MEASUREMENT_KEY = D.MEASUREMENT_KEY 
        limit 1000 ; 
    '''

    get_query_results(query, results_location)



    return True


def read_files_selected(date_selector, alarm_file, gen_info_file, geography):

    selected_date = datetime.datetime.strptime(date_selector, '%Y-%m-%d').date()


    site_list, pre_selection, dfs_general_info = validate_general_info_file(gen_info_file, geography)
    df_general_info_calc = dfs_general_info["Site Info"].set_index('Site')

    valid = validate_daily_alarms_report(date_selector, alarm_file)

    print(selected_date)
    print(valid)

    if not site_list:
        gr.Warning('THE GENERAL INFO FILE IS NOT WHAT WE EXPECTED')
    if not valid:
        gr.Warning('THE DAILY ALARM FILE IS NOtT WHAT WE EXPECTED')

    # QUERY THE DATA LAKE FOR IRR VALUES
    irr = query_single_day_irradiance(selected_date)

    # RUN THE PROCESS TO GET INCIDENT LISTS
    inc_list = None
    tracker_list = None

    inc_list_file = 'main.py'
    trkr_list_file = 'main.py'

    return (irr,
            gr.CheckboxGroup.update(choices=site_list, value=pre_selection, interactive=True),
            dfs_general_info,
            df_general_info_calc)


def read_daily_alarm_report(alarm_file):

    df_all = pd.read_excel(alarm_file.name, engine="openpyxl")
    df_all['InSolar Check'] = ""
    df_all['Curtailment Event'] = ""
    df_all['Tracker'] = ''
    df_all['Comments'] = ''

    return df_all


def create_incidents_lists(alarm_file, irr, site_list, geography, date_selector, dfs_general_info, df_general_info_calc):
    print(site_list)
    selected_date = datetime.datetime.strptime(date_selector, '%Y-%m-%d').date()

    df_all = read_daily_alarm_report(alarm_file)
    df_list_active, df_list_closed = data_treatment.create_dfs(df_all, site_list)
    df_tracker_active, df_tracker_closed = data_treatment.create_tracker_dfs(df_all, df_general_info_calc)

    print(df_tracker_active)

    # RUN THE PROCESS TO GET INCIDENT LISTS
    inc_list = None
    tracker_list = None

    inc_list_file = 'Incidents' + str(selected_date) + '-' + str(geography) + '.xlsx'
    trkr_list_file = 'Tracker_Incidents' + str(selected_date) + '-' + str(geography) + '.xlsx'

    return (gr.File.update(inc_list_file, interactive=False, visible=True),
            gr.File.update(trkr_list_file, interactive=False, visible=True))


with gr.Blocks() as demo:
    gr.Markdown('## Event Tracker Database Tool')

    SINGLE_DAY_IRR = gr.State([])
    dfs_general_info = gr.State([])
    df_general_info_calc = gr.State([])

    date_selector = gr.Dropdown(
        choices=LAST_30_DAYS, value=LAST_30_DAYS[-1])

    geo_selector = gr.Dropdown(
        choices=["USA", "AUS", "ES", "UK"], value="USA", label="Select Geography")

    with gr.Row():
        daily_alarm_report_upload = gr.File(label='Upload Daily Alarm Report',
                                            file_types=['.xlsx', '.xls', '.xlsm'])

        gen_info_file = gr.File(label='Upload General Info Excel',
                                file_types=['.xlsx', '.xls', '.xlsm'])

    read_files_btn = gr.Button('Read files and query relevant data')

    with gr.Row():
        user_site_select = gr.CheckboxGroup(choices=["No sites available yet"], value=["No sites available yet"], label="Select Sites", interactive=False)

    generate_files_btn = gr.Button('Generate Files')

    with gr.Row():
        inc_list_down = gr.File(interactive=False, visible=False)
        trkr_inc_list_down = gr.File(interactive=False, visible=False)


    # DYNAMIC LOGIC
    read_files_btn.click(read_files_selected,
                         [date_selector, daily_alarm_report_upload, gen_info_file, geo_selector],
                         [SINGLE_DAY_IRR, user_site_select, dfs_general_info, df_general_info_calc])

    generate_files_btn.click(create_incidents_lists,
                             [daily_alarm_report_upload, SINGLE_DAY_IRR, user_site_select, geo_selector, date_selector,
                              dfs_general_info, df_general_info_calc],
                             [inc_list_down, trkr_inc_list_down])



demo.queue()
demo.launch()
