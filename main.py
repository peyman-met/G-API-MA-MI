import aiohttp
import asyncio
import streamlit as st
import pandas as pd
from datetime import datetime
# import networkx as nx
# from networkx.algorithms import community
# from urllib.parse import urlparse


st.header('Google search results from AvesAPI')

api_key = st.text_input(label='Aves API key:', max_chars=28)

mode = st.radio(
    "Which format do you want to input your data?",
    ('Text', 'CSV'))

query_list = []
if mode == 'Text':
    input_text = st.text_area(label='Enter keywords:',
                              placeholder='''Keyword1\nKeyword2''', height=200)
    query_list = input_text.split('\n')

else:
    try:
        uploaded_file = st.file_uploader("Choose your file:",)
        all_df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        headers = all_df.columns.to_list()
        keyword_col = st.selectbox(
            label='Select KEYWORD column name:', options=headers)
        query_list = list(all_df[keyword_col].unique())
    except:
        pass


list_end = len(query_list)
st.write(f"we found {len(query_list)} rows!")
st.write(query_list[:10])

step = 5000

final_df = pd.DataFrame()
logList = []
errorList = []

if st.button('Start'):
    for start in range(0, list_end+1, step):
        stop = start+step
        if stop > list_end:
            stop = list_end
        keyword_list = query_list[start:stop]

        async def main():
            resultList = []
            session_timeout =  aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(timeout=session_timeout,trust_env=True) as session:
                tasks = []
                for kw in keyword_list:
                    task = asyncio.ensure_future(get_data(session, kw))
                    tasks.append(task)

                result = await asyncio.gather(*tasks)
                # logList.extend(result)

            for item in result:
                if not (item['request']['success']):
                    logList.append(item)
                    errorList.append(item['request_option']['keyword'])

                try:
                    organicResults = item['result']['organic_results']
                    resultLinks = []
                    resultLinks.append(item['search_parameters']['query'])

                    for r in organicResults:
                        resultLinks = resultLinks + [r['url']]

                    resultList.append(resultLinks)
                    temp_df = pd.DataFrame(resultList)

                except Exception as e:
                    print(e)
                    pass
            return temp_df

        async def get_data(session, keyword):
            url = f'https://api.avesapi.com/search?apikey={api_key}&engine=google&type=web&query={keyword}&google_domain=google.com&gl=ir&hl=fa&device=mobile&output=json&num=100'
            async with session.get(url, ssl=True) as resp:
                result_data = await resp.json()
                return result_data

        text = st.subheader(
            'Sending request. It might take a while, So please wait!')
        temp_df = asyncio.run(main())
        final_df = pd.concat([final_df, temp_df])

    # save list of keywords with error to file
    dfe = pd.DataFrame(errorList)
    dfe.to_csv('Keywords_Error_List.csv', index=False,
               encoding='utf-8-sig', mode='a', header=False)
    with open('Log.txt', 'w+', encoding='utf-8-sig') as fl:
        fl.write("\n".join(str(item) for item in logList))

    text.subheader('Done! You can download it now:')

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv(index=False).encode('utf-8-sig')

    csv = convert_df(final_df)
    today = datetime.today()
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name=f'Search_Results_{str(today)}.csv',
        mime='text/csv',
    )
