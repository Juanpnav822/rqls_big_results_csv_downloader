import os, json, requests, csv, time

ak= os.environ.get("ACCESS_KEY")
secret = os.environ.get("SECRET")

def token():
    url="https://api4.prismacloud.io/login"
    payload={
        "username":ak,
        "password":secret
    }
    headers={
        'Content-Type': 'application/json; charset=UTF-8',
        'Accept': 'application/json; charset=UTF-8'
    }

    response=requests.request("POST",url,headers=headers,data=json.dumps(payload))
    response=json.loads(response.content)

    return response['token']

def search_async(rql):
    url = "https://api4.prismacloud.io/search/api/v1/config/async"
    payload={
        "query":rql
    }
    headers={
        'Accept': 'application/json; charset=UTF-8',
        'x-redlock-auth': token()
    }
    response=requests.request("POST",url,headers=headers,json=payload)
    response=json.loads(response.content)
    return response

def status_of_report(status_url):
    url="https://api4.prismacloud.io{}".format(status_url)
    headers={
        'Accept': 'application/json; charset=UTF-8',
        'x-redlock-auth': token()
    }
    response=requests.request("GET",url,headers=headers)
    response=json.loads(response.text)
    return response

def download_csv_report_from_rql(report_url):
    url="https://api4.prismacloud.io{}".format(report_url)
    headers={
        'Accept': 'text/csv',
        'x-redlock-auth': token()
    }

    response=requests.request("GET",url,headers=headers)
    response=response.content
    return response

def handler():
    rql="""config from cloud.resource where resource.status = Active AND cloud.type = 'aws'"""
    reports_urls=search_async(rql)
    status_url=reports_urls['statusUrl']
    download_url=reports_urls['downloadUrl']

    status=status_of_report(status_url)
    status=status['status']
    print(status)

    while status!="COMPLETED":
        status=status_of_report(status_url)
        status=status['status']
    
    print(status)

    rql_results=download_csv_report_from_rql(download_url)

    with open("my_data.csv", "w", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([rql_results])

handler()
