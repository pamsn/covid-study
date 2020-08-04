import csv
import logging
import os
import pprint
import re

import requests
import subprocess
import threading
import time

import git
import pandas as pd

from pandas.io.json import json_normalize
from datetime import datetime

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

logging.basicConfig(filename='/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/logging.log', level=logging.DEBUG,format='[%(levelname)s] %(threadName)s %(message)s', )


def branches_of_repo_github_v2(owner, repo, api):
    branches = []
    next = True
    i = 1
    global total_requests
    gh_session = requests.Session()
    gh_session.auth = ('pamsn', "797130c04f13bccb6ed63322143cb94aa7129ebe")
    while next == True:
        total_requests = total_requests + 1
        if total_requests > 4500:
            time.sleep(60 * 60)
            print("Sleeping...")
            total_requests = 0
        url = api + '/repos/{}/{}/branches?page={}&per_page=100&access_token={}'.format(owner, repo, i,"797130c04f13bccb6ed63322143cb94aa7129ebe")
        time.sleep(1)
        branche_pg = gh_session.get(url = url, headers = headers)
        branche_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in branche_pg.json()]
        branche_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in branche_pg_list]
        branches = branches + branche_pg_list
        if 'Link' in branche_pg.headers:
            if 'rel="next"' not in branche_pg.headers['Link']:
                next = False
        i = i + 1
    return branches

def create_branches_df_v2(owner,repo, api):
    branches_list = branches_of_repo_github_v2(owner, repo, api)
    return json_normalize(branches_list)

def number_of_branches_day(owner,repo,github_api):
    print('--------------------BRANCHES PER daY------------------------------')
    branches = create_branches_df_v2(owner,repo,github_api)
    # print(branches.to_string())
    list_of_branche_commits = branches['commit.url'].tolist()
    date_list =[]
    items_list=[]
    for url in list_of_branche_commits:
        repositorie_pg = gh_session.get(url = url, headers = headers) #requests.get(url)
        # print(repositorie_pg.json())
        items_list.append(repositorie_pg.json()['commit'])
    for item in items_list:
        for key,value in item.items():
            if key == 'committer':
                match = re.search(r'\d{4}-\d{2}-\d{2}', value['date'])
                date = datetime.strptime(match.group(), '%Y-%m-%d').date()
                # print(date)
                date_list.append(date)
    branches_first_interval = []
    branches_second_interval = []
    for date in date_list:
        if datetime.strptime(str(start_date), "%Y-%m-%d").date() < date < datetime.strptime(str(end_date), "%Y-%m-%d").date():
            branches_first_interval.append(date)

    if branches_first_interval != []:
        s = pd.to_datetime(pd.Series(branches_first_interval), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict = dict(zip(s.index.format(), s))
    else:
        result_dict = {}

    owner = url.split("/")[4]
    repo = url.split("/")[5]
    result_dict['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/branch_api.csv')
    print(list_dicts)












def get_rate_limit(api):
    url = api + '/rate_limit'
    rate_limit_pg = gh_session.get(url=url, headers = headers)
    return int(rate_limit_pg.json()['rate']['remaining'])

def save_csv_file(list_dicts,path):

    done_analysis = []
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for dct in map(dict, reader):
            done_analysis.append(dct)

    final = done_analysis + list_dicts

    # keys = final[0].keys()
    with open(path, 'w+') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=['url','2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08',
                                                              '2019-09', '2019-10', '2019-11', '2019-12', '2020-01', '2020-02', '2020-03', '2020-04',
                                                              '2020-05'])
        dict_writer.writeheader()
        dict_writer.writerows(final)
    output_file.close()

def issues_of_repo_github(owner,repo, api):
    issues = []
    next = True
    i = 1
    global total_requests
    while next == True:
        total_requests = total_requests + 1
        if total_requests > 4500:
            time.sleep(60 * 60)
            print("Sleeping...")
            total_requests = 0
        url = api + '/repos/{}/{}/issues?state=all&page={}&per_page=100&access_token={}'.format(owner,repo , i,"797130c04f13bccb6ed63322143cb94aa7129ebe")
        time.sleep(1)
        issue_pg = gh_session.get(url = url, headers = headers)
        issue_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in issue_pg.json()]
        issue_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in issue_pg_list]
        issues = issues + issue_pg_list
        if 'Link' in issue_pg.headers:
            if 'rel="next"' not in issue_pg.headers['Link']:
                next = False
        i = i + 1
    return issues

def create_issues_df(owner, repo, api):
    issues_list = issues_of_repo_github(owner, repo, api)
    return json_normalize(issues_list)

def pull_requests_and_issues_month(owner,repo,github_api,e2):
    logging.debug('Starting PULL REQUESTS AND ISSUES')

    issues = create_issues_df(owner,repo,github_api)
    issues['type'] = ['issue' if str(x) =='nan' else 'pull request' for x in issues['pull_request.url']]
    df_pull_requests = issues[issues['type'] == 'pull request']
    df_issues = issues[issues['type'] == 'issue']
    # print("-------------------- ISSUES ------------------------------")
    # print("-----OPEN ISSUES ------")
    # print(df_issues.head().to_string())
    df_open_issues = df_issues[df_issues['state'] == 'open']
    import pandas as pd
    pd.options.mode.chained_assignment = None
    # df_open_issues['date'] = pd.to_datetime(df_open_issues['created_at'])
    # df_open_issues['date'] = pd.to_datetime(df_open_issues['date'], utc=True)
    # df_open_issues['issue_created_date'] = df_open_issues['date'].dt.date
    df_open_issues['date'] = df_open_issues['created_at'].str[:10]
    df_open_issues['issue_created_date'] = pd.to_datetime(df_open_issues['date'])
    mask = (df_open_issues['issue_created_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_open_issues['issue_created_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_open_issues = df_open_issues.loc[mask]
    col_issue_created_per_date = df_open_issues['issue_created_date'].tolist()
    if col_issue_created_per_date != []:
        s = pd.to_datetime(pd.Series(col_issue_created_per_date), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_created_issues = dict(zip(s.index.format(), s))
    else:
        result_dict_created_issues = {}


    # print("-----CLOSED ISSUES ------")
    ## issues closed per day
    df_closed_issues = df_issues[df_issues['state'] == 'closed']
    # df_closed_issues['date'] = pd.to_datetime(df_closed_issues['closed_at'])
    # df_closed_issues['date'] = pd.to_datetime(df_closed_issues['date'], utc=True)
    # df_closed_issues['issue_closed_date'] = df_closed_issues['date'].dt.date
    df_closed_issues['date'] = df_closed_issues['closed_at'].str[:10]
    df_closed_issues['issue_closed_date'] = pd.to_datetime(df_closed_issues['date'])
    mask = (df_closed_issues['issue_closed_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_closed_issues['issue_closed_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_closed_issues = df_closed_issues.loc[mask]
    col_closed_issue_per_date = df_closed_issues['issue_closed_date'].tolist()
    if col_closed_issue_per_date !=[]:
        s = pd.to_datetime(pd.Series(col_closed_issue_per_date), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_closed_issues = dict(zip(s.index.format(), s))
    else:
        result_dict_closed_issues = {}

    # print("--------------------PULL REQUESTS------------------------------")
    # print("----- OPEN PULL REQUESTS -----")
    # pull requests created per day
    df_open_pull_requests = df_pull_requests[df_pull_requests['state'] == 'open']
    # print(df_open_issues.head().to_string())
    import pandas as pd
    pd.options.mode.chained_assignment = None
    # df_open_pull_requests['date'] = pd.to_datetime(df_open_pull_requests['created_at'])
    # df_open_pull_requests['date'] = pd.to_datetime(df_open_pull_requests['date'], utc=True)
    # df_open_pull_requests['pull_request_create_date'] = df_open_pull_requests['date'].dt.date
    df_open_pull_requests['date'] = df_open_pull_requests['created_at'].str[:10]
    df_open_pull_requests['pull_request_create_date'] = pd.to_datetime(df_open_pull_requests['date'])
    mask = (df_open_pull_requests['pull_request_create_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_open_pull_requests['pull_request_create_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_open_pull_requests = df_open_pull_requests.loc[mask]
    col_pull_requests_created_per_date = df_open_pull_requests['pull_request_create_date'].tolist()

    if col_pull_requests_created_per_date != []:
        s = pd.to_datetime(pd.Series(col_pull_requests_created_per_date), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_created_pull_requests = dict(zip(s.index.format(), s))
    else :
        result_dict_created_pull_requests = {}

    # print("-----CLOSED PULL REQUESTS ------")
    ## issues closed per day
    df_closed_pull_requests = df_pull_requests[df_pull_requests['state'] == 'closed']
    # df_closed_pull_requests['date'] = pd.to_datetime(df_closed_pull_requests['closed_at'])
    # df_closed_pull_requests['date'] = pd.to_datetime(df_closed_pull_requests['date'], utc=True)
    # df_closed_pull_requests['pull_request_close_date'] = df_closed_pull_requests['date'].dt.date
    df_closed_pull_requests['date'] = df_closed_pull_requests['closed_at'].str[:10]
    df_closed_pull_requests['pull_request_close_date'] = pd.to_datetime(df_closed_pull_requests['date'])
    mask = (df_closed_pull_requests['pull_request_close_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_closed_pull_requests['pull_request_close_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_closed_pull_requests = df_closed_pull_requests.loc[mask]
    col_closed_pull_requests_per_date = df_closed_pull_requests['pull_request_close_date'].tolist()
    if col_closed_pull_requests_per_date != []:
        s = pd.to_datetime(pd.Series(col_closed_pull_requests_per_date), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_closed_pull_requests = dict(zip(s.index.format(), s))
    else:
        result_dict_closed_pull_requests = {}


    result_dict_created_issues['url'] = "https://github.com/" +owner +"/"+repo + ".git"
    list_dicts = [result_dict_created_issues]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/created_issues_info.csv')
    list_dicts =[]
    result_dict_closed_issues['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict_closed_issues]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/closed_issues_info.csv')
    list_dicts = []
    result_dict_created_pull_requests['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict_created_pull_requests]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/created_pull_requests_info.csv')
    list_dicts = []
    result_dict_closed_pull_requests['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict_closed_pull_requests]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/closed_pull_requests_info.csv')
    list_dicts =[]

    # print("-----TOTAL OPEN AND CLOSE ISSUES ------")
    df_issues = issues[issues['type'] == 'issue']
    df_issues['date'] = df_issues['created_at'].str[:10]
    df_issues['issue_created_date'] = pd.to_datetime(df_issues['date'])
    df_issues['date'] = df_issues['closed_at'].str[:10]
    df_issues['issue_closed_date'] = pd.to_datetime(df_issues['date'])
    mask = (df_issues['issue_created_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_issues['issue_created_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_issues = df_issues.loc[mask]
    mask = (df_issues['issue_closed_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_issues['issue_closed_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_issues = df_issues.loc[mask]

    df_issues['month_year_created_at'] = pd.to_datetime(df_issues['issue_created_date']).dt.to_period('M')
    df_issues['month_year_closed_at'] = pd.to_datetime(df_issues['issue_closed_date']).dt.to_period('M')

    df_issues['equal'] = pd.np.where((df_issues['month_year_created_at'] == df_issues['month_year_closed_at']), df_issues['month_year_created_at'].dt.strftime('%Y-%m'), pd.np.nan)
    # print(df_issues.head().to_string())

    col_issues_opened_closed_same_date = df_issues['equal'].tolist()
    if col_issues_opened_closed_same_date != []:
        s = pd.to_datetime(pd.Series(col_issues_opened_closed_same_date), format='%Y/%m')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_issues_opened_closed_same_date = dict(zip(s.index.format(), s))
    else:
        result_dict_issues_opened_closed_same_date = {}

    result_dict_issues_opened_closed_same_date['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict_issues_opened_closed_same_date]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/issues_opened_closed_same_month.csv')
    list_dicts = []

    # print("-----TOTAL OPEN AND CLOSE PULL REQUESTS ------")
    df_pull_requests = issues[issues['type'] == 'pull request']
    df_pull_requests['date'] = df_pull_requests['created_at'].str[:10]
    df_pull_requests['pull_request_created_date'] = pd.to_datetime(df_pull_requests['date'])
    df_pull_requests['date'] = df_pull_requests['closed_at'].str[:10]
    df_pull_requests['pull_request_closed_date'] = pd.to_datetime(df_pull_requests['date'])
    mask = (df_pull_requests['pull_request_created_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_pull_requests['pull_request_created_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_pull_requests = df_pull_requests.loc[mask]
    mask = (df_pull_requests['pull_request_closed_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (df_pull_requests['pull_request_closed_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    df_pull_requests = df_pull_requests.loc[mask]

    df_pull_requests['month_year_created_at'] = pd.to_datetime(df_pull_requests['pull_request_created_date']).dt.to_period('M')
    df_pull_requests['month_year_closed_at'] = pd.to_datetime(df_pull_requests['pull_request_closed_date']).dt.to_period('M')

    df_pull_requests['equal'] = pd.np.where((df_pull_requests['month_year_created_at'] == df_pull_requests['month_year_closed_at']), df_pull_requests['month_year_created_at'].dt.strftime('%Y-%m'), pd.np.nan)
    # print(df_issues.head().to_string())

    col_pull_request_opened_closed_same_month = df_pull_requests['equal'].tolist()
    if col_pull_request_opened_closed_same_month != []:
        s = pd.to_datetime(pd.Series(col_pull_request_opened_closed_same_month), format='%Y/%m')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict_pull_requests_opened_closed_same_month = dict(zip(s.index.format(), s))
    else:
        result_dict_pull_requests_opened_closed_same_month = {}

    result_dict_pull_requests_opened_closed_same_month['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict_pull_requests_opened_closed_same_month]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/pull_requests_opened_closed_same_month.csv')
    list_dicts = []
    e2.set()
    logging.debug('Exiting PULL REQUESTS AND ISSUES')

def change_date(date_commit):
    from email.utils import parsedate
    from datetime import datetime
    import time
    t = parsedate(date_commit)
    final_format = datetime.fromtimestamp(time.mktime(t)).strftime("%m/%d/%Y, %H:%M:%S")
    return pd.Timestamp(final_format)

def get_commits_from_file(project_path, commits_folder, url):
    dataframe = pd.read_csv(project_path + '/' + commits_folder, delimiter='$')

    dataframe.columns = ['commit_hash', 'email', 'commit_date']
    # dataframe['date'] = pd.to_datetime(dataframe['commit_date'])
    # dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True)
    # dataframe['date'] = dataframe['date'].dt.date
    dataframe['date'] = dataframe['commit_date'].str[:10]

    dataframe['date'] = pd.to_datetime(dataframe['date'])
    mask = (dataframe['date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (dataframe['date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    dataframe = dataframe.loc[mask]
    list_of_commits = dataframe['date'].tolist()
    s = pd.to_datetime(pd.Series(list_of_commits), format='%Y/%m/%d')
    s.index = s.dt.to_period('m')
    s = s.groupby(level=0).size()
    s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
    result_dict = dict(zip(s.index.format(), s))
    result_dict['url'] = url
    return result_dict

def get_commits(url,e):
    logging.debug('Starting COMMITS')
    folder = '/Volumes/LaCie/Downloaded_projects/'
    commits_file = "commits_file.csv"
    output = '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/commit_info_projects.csv'

    path = folder + url.split("/")[4].replace(".git", "")
    # print(path)
    subprocess.check_output('git log --all --format=%H$%aE$%ci > ' + commits_file, cwd=path, shell=True)
    dataframe = pd.read_csv(path + '/' + commits_file, delimiter='$')
    dataframe.columns = ['commit_hash', 'email', 'commit_date']
    if os.path.exists(path + '/' + commits_file):
        os.remove(path + '/' + commits_file)
    dataframe.to_csv(path + '/' + commits_file, index=False, header=True, encoding="utf-8", sep='$')


    result_dict = get_commits_from_file(path, commits_file, url)
    e.set()
    analyzed_projects = []
    with open(output) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for dct in map(dict, reader):
            analyzed_projects.append(dct)

    final = analyzed_projects + [result_dict]

    with open(output, 'w+') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=['url', '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09',
                                                              '2019-10', '2019-11', '2019-12', '2020-01', '2020-02', '2020-03', '2020-04', '2020-05'])
        dict_writer.writeheader()
        dict_writer.writerows(final)
    output_file.close()

    logging.debug('Exiting COMMITS')

def clone(path,url,e):
    folder = '/Volumes/LaCie/Downloaded_projects'
    if not os.path.exists(folder + "/" + url.split("/")[4].replace(".git", "") + "/"):
        git.Git(path).clone(url)

def get_developers_emails(url):
    owner = url.split("/")[3]
    repo = url.split("/")[4].replace(".git", "")
    if not os.path.exists('/Volumes/LaCie/Downloaded_projects/' + owner + "'__'" + repo + '/commits_file.csv'):
        folder = '/Volumes/LaCie/Downloaded_projects'
        commits_file = "commits_file.csv"
        path = folder + "/" + owner + "'__'" + repo
        subprocess.check_output('git log --all --format=%H$%aE$%ci > ' + commits_file, cwd=path, shell=True)

    dataframe = pd.read_csv('/Volumes/LaCie/Downloaded_projects/' + owner + "'__'" + repo + '/commits_file.csv', delimiter='$')
    dataframe.columns = ['commit_hash', 'email', 'commit_date']
    list_of_developers = dataframe['email'].tolist()
    from collections import Counter
    import operator
    # dict_occurrences = dict(Counter(list_of_developers))
    # sorted_d = sorted(dict_occurrences.items(), key=lambda x: x[1], reverse=True)
    # print(sorted_d)
    # mylist = list(dict.fromkeys(list_of_developers))
    final_list =[]
    resultado = []
    list_of_developers = list(dict.fromkeys(list_of_developers))
    print(list_of_developers)

    for item in list_of_developers:
        if str(item) != 'nan' and str(item) != '' and str(item) != None and "noreply" not in str(item) :
            final_list.append(item)
        # count = count + 1
        # if count == 10:
        #     break
    return(final_list)




def get_new_developers_from_file(url,e):
    e.wait()
    logging.debug('Starting NEW DEVELOPERS')
    if not os.path.exists('/Volumes/LaCie/Downloaded_projects/'+ url.split("/")[4].replace(".git", "")+'/commits_file.csv'):
        folder = '/Volumes/LaCie/Downloaded_projects/'
        commits_file = "commits_file.csv"
        path = folder + "/" + url.split("/")[4].replace(".git", "")
        subprocess.check_output('git log --all --format=%H$%aE$%ci > ' + commits_file, cwd=path, shell=True)

    dataframe = pd.read_csv('/Volumes/LaCie/Downloaded_projects/'+ url.split("/")[4].replace(".git", "")+'/commits_file.csv', delimiter='$')
    dataframe.columns = ['commit_hash', 'email', 'commit_date']

    # dataframe['date'] = pd.to_datetime(dataframe['commit_date'])
    # dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True)
    # dataframe['date'] = dataframe['date'].dt.date
    dataframe['date'] = dataframe['commit_date'].str[:10]
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    mask = (dataframe['date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (dataframe['date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    commits_inside_interval = dataframe.loc[mask]

    mask = (dataframe['date'] < datetime.strptime(str(start_date), "%Y-%m-%d").date())
    commits_before_interval = dataframe.loc[mask]

    list_of_old_developers = commits_before_interval['email'].tolist()

    #retira as linhas que tem algum desenvolvedor antigo (aquele que apareceu antes do intervalo de analise)
    new_developers_dataframe = commits_inside_interval[~commits_inside_interval.email.isin(list_of_old_developers)]
    #https://stackoverflow.com/questions/24136620/python-pandas-keeping-only-dataframe-rows-containing-first-occurrence-of-an-ite
    #pegando a primeira ocorrencia de cada novo desenvolvedor.
    new_developers_dataframe = new_developers_dataframe.sort_values(by='commit_date').drop_duplicates(subset=['email'])

    new_developers_list = new_developers_dataframe['date'].tolist()

    if new_developers_list != []:
        s = pd.to_datetime(pd.Series(new_developers_list), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict = dict(zip(s.index.format(), s))
    else:
        result_dict = {}

    owner = url.split("/")[3]
    repo = url.split("/")[4].replace(".git", "")
    result_dict['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/new_developers_month.csv')
    list_dicts = []

    logging.debug('Exiting NEW DEVELOPERS')

def get_active_contributors_from_file(ur):
    if not os.path.exists('/Volumes/LaCie/Downloaded_projects/' + url.split("/")[4].replace(".git", "") + '/commits_file.csv'):
        folder = '/Volumes/LaCie/Downloaded_projects/'
        commits_file = "commits_file.csv"
        path = folder + "/" + url.split("/")[4].replace(".git", "")
        subprocess.check_output('git log --all --format=%H$%aE$%ci > ' + commits_file, cwd=path, shell=True)

    dataframe = pd.read_csv('/Volumes/LaCie/Downloaded_projects/' + url.split("/")[4].replace(".git", "") + '/commits_file.csv', delimiter='$')
    dataframe.columns = ['commit_hash', 'email', 'commit_date']

    # print(dataframe.head().to_string())

    # dataframe['date'] = pd.to_datetime(dataframe['commit_date'])
    dataframe['date'] = dataframe['commit_date'].str[:10]
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    # dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True)
    # dataframe['date'] = dataframe['date'].dt.date
    mask = (dataframe['date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (dataframe['date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    commits_inside_interval = dataframe.loc[mask]

    #https://jamesrledoux.com/code/drop_duplicates
    #removendo desenvolvedores que contribuiram mais de uma vez no mesmo dia (deixando apenas uma ocorrencia dele   )

    commits_inside_interval = commits_inside_interval.drop_duplicates(subset=['email'])
    active_contributors_list = commits_inside_interval['date'].tolist()

    if active_contributors_list != []:
        s = pd.to_datetime(pd.Series(active_contributors_list), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict = dict(zip(s.index.format(), s))
    else:
        result_dict = {}

    owner = url.split("/")[3]
    repo = url.split("/")[4].replace(".git", "")
    result_dict['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/active_contributors_info_v2.csv')
    list_dicts = []

def get_branch_creation_date(path):
    # print(path)
    dataframe = pd.read_csv(path, delimiter='$')
    dataframe.columns = ['commit_hash', 'email', 'commit_date']
    # dataframe['date'] = pd.to_datetime(dataframe['commit_date'])
    # dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True)
    # dataframe['date'] = dataframe['date'].dt.date
    dataframe['date'] = dataframe['commit_date'].str[:10]
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    # print(dataframe.to_string())
    # mask = (dataframe['date'].date() > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (dataframe['date'].date() <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    # dataframe = dataframe.loc[mask]
    # print(dataframe.head().to_string())
    sorted_commits = dataframe.sort_values(by=['commit_date'], ascending=True)
    # dataframe = None
    # print(sorted_commits.head().to_string())

    # print(sorted_commits.head().to_string())
    import os

    os.remove(path)
    if len(sorted_commits) != 0:
        return sorted_commits['date'].iloc[0]
    else:
        return 0

def get_branches_from_file(url):
    # e.wait()
    # logging.debug('Starting BRANCHES')
    list_branches_dates=[]
    # if not os.path.exists('/Volumes/LaCie/Downloaded_projects/' + url.split("/")[4].replace(".git", "") + '/branches_file.csv'):
    folder = '/Volumes/LaCie/Downloaded_projects/'
    branches_file = "branches_filev2.csv"
    path = folder + url.split("/")[4].replace(".git", "")
    # subprocess.check_output('git reflog --date=short --format=%H$%aE$%ci > ' + branches_file, cwd=path, shell=True)
    subprocess.check_output('git branch -r --list > ' + branches_file, cwd=path, shell=True)
    file1 = open('/Volumes/LaCie/Downloaded_projects/' + url.split("/")[4].replace(".git", "") + '/branches_filev2.csv', 'r')
    Lines = file1.readlines()
    # print("URLLLLLLLL: "+ url)
    number_of_branches = len(Lines)
    for line in Lines:
        if 'origin/HEAD ->' not in line.strip():
            subprocess.check_output('git log '+ line.strip() + ' --first-parent --format=%H$%aE$%ci > ' + str(number_of_branches) +".csv", cwd=path, shell=True)
            # print('git log '+ line.strip() + ' --first-parent --format=%H$%aE$%ci > ' + "branch_file_temp.csv")
            # print(line.strip())
            branch_date = get_branch_creation_date(path + "/"+ str(number_of_branches) +".csv")
            if branch_date != 0:
                if (branch_date.date() > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (branch_date.date() <= datetime.strptime(str(end_date), "%Y-%m-%d").date()):
                    list_branches_dates.append(branch_date)
        else:
            line = 'origin/HEAD'
            subprocess.check_output('git log ' + line + ' --first-parent --format=%H$%aE$%ci > ' + str(number_of_branches) +".csv", cwd=path, shell=True)
            branch_date = get_branch_creation_date(path + "/"+ str(number_of_branches) +".csv")
            if branch_date != 0:
                if (branch_date.date() > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (branch_date.date() <= datetime.strptime(str(end_date), "%Y-%m-%d").date()):
                    list_branches_dates.append(branch_date)
        number_of_branches = number_of_branches - 1

    if list_branches_dates != []:
        s = pd.to_datetime(pd.Series(list_branches_dates), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict = dict(zip(s.index.format(), s))
    else:
        result_dict = {}

    owner = url.split("/")[3]
    repo = url.split("/")[4].replace(".git", "")
    print(owner)
    print(repo)
    result_dict['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/branches_infov_ascending.csv')
    list_dicts = []
    # logging.debug('Exiting BRANCHES')

def pull_requests_comments_of_repo_github(owner, repo, api):
    pull_requests = []
    next = True
    i = 1
    while next == True:
        url = api + '/repos/{}/{}/pulls/comments?page={}&per_page=100&access_token={}'.format(owner, repo, i,"797130c04f13bccb6ed63322143cb94aa7129ebe")
        time.sleep(4)
        print(url)
        print(get_rate_limit(api))
        try:
            pull_requests_comments_pg = gh_session.get(url = url, headers = headers)
            pull_requests_comments_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in pull_requests_comments_pg.json()]
            pull_requests_comments_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in pull_requests_comments_pg_list]
            pull_requests = pull_requests + pull_requests_comments_pg_list
            if 'Link' in pull_requests_comments_pg.headers:
                if 'rel="next"' not in pull_requests_comments_pg.headers['Link']:
                    next = False
        except ValueError as e:
            print(e)
            continue
        i = i + 1
    return pull_requests

def create_pull_requests_comments_df(owner,repo, api):
    pull_requests_comments_list = pull_requests_comments_of_repo_github(owner, repo, api)
    return json_normalize(pull_requests_comments_list)



def pull_requests_comments(owner,repo,github_api):
    # e3.wait()
    # print('--------------------Repository Pull Requests Comments------------------------------')
    logging.debug('Starting PULL REQUESTS COMMENTS')
    pull_requests_comments = create_pull_requests_comments_df(owner,repo,github_api)

    pull_requests_comments['created_at_date'] = pull_requests_comments['created_at'].str[:10]
    pull_requests_comments['created_at_date'] = pd.to_datetime(pull_requests_comments['created_at_date'])

    pull_requests_comments['updated_at_date'] = pull_requests_comments['updated_at'].str[:10]
    pull_requests_comments['updated_at_date'] = pd.to_datetime(pull_requests_comments['updated_at_date'])

    mask = (pull_requests_comments['created_at_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (pull_requests_comments['created_at_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    pull_requests_comments_created_inside_interval = pull_requests_comments.loc[mask]
    mask = (pull_requests_comments['updated_at_date'] > datetime.strptime(str(start_date), "%Y-%m-%d").date()) & (pull_requests_comments['updated_at_date'] <= datetime.strptime(str(end_date), "%Y-%m-%d").date())
    pull_requests_comments_updated_inside_interval = pull_requests_comments.loc[mask]

    list_of_dates_pull_requests_comments_created = pull_requests_comments_created_inside_interval['created_at_date'].tolist()

    if list_of_dates_pull_requests_comments_created != []:
        s = pd.to_datetime(pd.Series(list_of_dates_pull_requests_comments_created), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict = dict(zip(s.index.format(), s))
    else:
        result_dict = {}

    result_dict['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/pull_requests_comments_created_info.csv')
    list_dicts = []

    list_of_dates_pull_requests_comments_updated = pull_requests_comments_updated_inside_interval['updated_at_date'].tolist()

    if list_of_dates_pull_requests_comments_updated != []:
        s = pd.to_datetime(pd.Series(list_of_dates_pull_requests_comments_updated), format='%Y/%m/%d')
        s.index = s.dt.to_period('m')
        s = s.groupby(level=0).size()
        s = s.reindex(pd.period_range(s.index.min(), s.index.max(), freq='m'), fill_value=0)
        result_dict = dict(zip(s.index.format(), s))
    else:
        result_dict = {}

    result_dict['url'] = "https://github.com/" + owner + "/" + repo + ".git"
    list_dicts = [result_dict]
    save_csv_file(list_dicts, '/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/pull_requests_comments_updated_info.csv')
    list_dicts = []

    logging.debug('Exiting PULL REQUESTS COMMENTS')

total_requests = 0
github_api = "https://api.github.com"
gh_session = requests.Session()
gh_session.auth = ("pamsnufrpe", "6444b3886a6e76b2fe406f161da11f748d62d329")
headers = {'user-agent': 'pamsnufrpe'}
remaining = get_rate_limit(github_api)
start_date = '2019-01-01'
end_date = '2020-05-19'
developers_full_list = []
def get_info(result):
    import csv
    folder = '/Users/belize/projetos_github/pandemia_project/projects'
    for url in result:
        print(url)
        logging.debug(url)
        path = folder
        get_active_contributors_from_file(url)
        # get_branches_from_file(url)
        # git.Git(path).clone(url)
        # e = threading.Event()
        # e2 = threading.Event()
        # e3 = threading.Event()
        # t1 = threading.Thread(name='t1', target=clone, args=(path,url,e))
        # t2 = threading.Thread(name='t2', target=pull_requests_and_issues_month, args=(url.split("/")[3],url.split("/")[4].replace(".git", ""),github_api,e2)) #using api
        # t3 = threading.Thread(name='t3', target=get_commits, args=(url,e)) #using local download (clone)
        # t4 = threading.Thread(name='t4', target=get_new_developers_from_file, args=(url,e)) #using local download (clone)
        # t5 = threading.Thread(name='t5', target=get_active_contributors_from_file, args=(url, e)) #using local download (clone)
        # t6 = threading.Thread(name='t6', target=get_branches_from_file, args=(url, e)) #using local download (clone)
        # t7 = threading.Thread(name='t7', target=issues_comments, args=(url.split("/")[3],url.split("/")[4].replace(".git", ""),github_api,e2,e3)) #using api
        # t7 = threading.Thread(name='t7', target=issues_comments, args=(url.split("/")[3], url.split("/")[4].replace(".git", ""), github_api))  # using api
        # t8 = threading.Thread(name='t8', target=pull_requests_comments, args=(url.split("/")[3],url.split("/")[4].replace(".git", ""),github_api)) #using api
        # get_branches_from_file(url)
        # t9 = threading.Thread(name='t9', target=get_developers_emails, args=(url,))
        # t1.start()
        # t2.start()
        # t3.start()
        # t4.start()
        # t5.start()
        # t6.start()
        # t7.start()
        # t8.start()
        # t9.start()

        # t1.join()
        # t2.join()
        # t3.join()
        # t4.join()
        # t5.join()
        # t6.join()
        # t7.join()
        # t8.join()
        # t9.join()
        # developers_full_list.append(get_developers_emails(url))
    # flat_list2 = []
    # for sublist in developers_full_list:
    #     for item in sublist:
    #         if item != "":
    #             flat_list2.append(item)
    import csv
    # print(len(flat_list2))
    # with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/emails3.csv", "a") as fp:
    #     wr = csv.writer(fp, dialect='excel')
    #     wr.writerow(flat_list2)
    # fp.close()

        # with open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/emails.csv', 'r') as f:
        #     l1 = []
        #     for ele in f:
        #         line = ele.split('\n')
        #         l1.append(line)
        # print(len(l1))
        # print(l1)
        # final_mylist = []
        #
        # for item2 in final_mylist:
        #     final_mylist.append(item2)
        #
        # final_mylist = list(dict.fromkeys(final_mylist))
        # with open("/Users/belize/projetos_github/pandemia_project/pandemia_v2/emails.csv", "w") as fp:
        #     wr = csv.writer(fp, dialect='excel')
        #     wr.writerow(final_mylist)

        # issues_comments(url.split("/")[3], url.split("/")[4].replace(".git", ""), github_api)
        #
        # if os.path.exists(folder + "/" + url.split("/")[4].replace(".git", "") + "/"):
        #     import shutil
        #     shutil.rmtree(folder + "/" + url.split("/")[4].replace(".git", ""))
#
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/closed_issues_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/closed_pull_requests_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/commit_info_projects.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/created_issues_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/created_pull_requests_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/new_developers_month.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/active_contributors_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/branches_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/issues_opened_closed_same_month.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/pull_requests_opened_closed_same_month.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/issues_comments_created_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/issues_comments_updated_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/pull_requests_comments_created_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/pull_requests_comments_updated_info.csv', 'w').close()
# open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/logging.log', 'w').close()

analyzed_projects = []
analyzed_projects2 = []
# analyzed_projects3 = []
#
projects_to_be_analyzed = [line.rstrip('\n') for line in open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/projects_checking.txt')]

# with open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/info_projects.csv') as csvfile:
#     reader = csv.DictReader(csvfile, delimiter=",")
#     for dct in map(dict, reader):
#         analyzed_projects.append(dct)

# with open('/Users/belize/projetos_github/pandemia_project/pandemia_v2/output/branch_api.csv') as csvfile:
#     reader = csv.DictReader(csvfile, delimiter=",")
#     for dct in map(dict, reader):
#         analyzed_projects2.append(dct)

analyzed_projects2=[]
result = list(set(projects_to_be_analyzed) - set([d['url'] for d in analyzed_projects2]))
# get_info(result)
# result = ['https://github.com/mapstruct/mapstruct.git']
for url in result:
    # number_of_branches_day(url.split("/")[3],url.split("/")[4].replace(".git", ""),github_api)
    # get_active_contributors_from_file(url)
    issues_comments(url.split("/")[3], url.split("/")[4].replace(".git", ""), github_api)
    # pull_requests_comments(url.split("/")[3], url.split("/")[4].replace(".git", ""), github_api)
# url = 'https://github.com/netty/netty.git'

# issues_comments(url.split("/")[3], url.split("/")[4].replace(".git", ""), github_api)
# issues_comments(url.split("/")[3], url.split("/")[4].replace(".git", ""), github_api)

# get_info(result)
# length = len(result)

# middle_index = length//2
#
# first_half = result[:middle_index]
# second_half = result[middle_index:]

# first_half =['https://github.com/robolectric/robolectric.git']
# second_half =['https://github.com/Angel-ML/angel.git']

# ta = threading.Thread(name='ta', target=get_info, args=(result,))
# tb = threading.Thread(name='tb', target=get_info, args=(second_half,))
# ta.start()
# tb.start()

# print(get_rate_limit(github_api))
