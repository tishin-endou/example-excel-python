# pip install pandas openpyxl paramiko psycopg2-binary pymysql Office365-REST-Python-Client

import paramiko
import pandas as pd
import psycopg2
import pymysql
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext

# SSHポートフォワーディング設定
def setup_ssh_tunnel(ssh_host, ssh_port, ssh_user, ssh_key_path, local_port, remote_host, remote_port):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=ssh_host, port=ssh_port, username=ssh_user, key_filename=ssh_key_path)

    tunnel = ssh_client.get_transport().open_channel(
        "direct-tcpip",
        (remote_host, remote_port),
        ("127.0.0.1", local_port),
    )
    return ssh_client, tunnel

# Redshiftデータ抽出
def fetch_redshift_data(query):
    conn = psycopg2.connect(
        host="127.0.0.1", port=5439, user="your_redshift_user",
        password="your_password", database="your_database"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# RDSデータ抽出
def fetch_rds_data(query):
    conn = pymysql.connect(
        host="127.0.0.1", port=3306, user="your_rds_user",
        password="your_password", database="your_database"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# SharePointにアップロード
def upload_to_sharepoint(site_url, username, password, local_path, folder_url):
    auth_context = AuthenticationContext(site_url)
    if auth_context.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, auth_context)
        target_folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        with open(local_path, 'rb') as file_content:
            target_file_name = local_path.split('/')[-1]
            target_folder.upload_file(target_file_name, file_content).execute_query()
    else:
        print("SharePoint authentication failed.")

# SharePointファイルリネーム
def rename_file_on_sharepoint(site_url, username, password, folder_url, old_file_name, new_file_name):
    auth_context = AuthenticationContext(site_url)
    if auth_context.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, auth_context)
        folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        file = folder.get_file_by_server_relative_url(f"{folder_url}/{old_file_name}")
        file.rename(new_file_name).execute_query()
    else:
        print("SharePoint authentication failed.")

# メイン処理
def main():
    # SSH設定
    ssh_host = "your_bastion_host"
    ssh_port = 1192
    ssh_user = "your_ssh_user"
    ssh_key_path = "path/to/your/private/key"
    local_port_redshift = 5439
    remote_host_redshift = "redshift-cluster-endpoint"
    remote_port_redshift = 5439
    local_port_rds = 3306
    remote_host_rds = "rds-endpoint"
    remote_port_rds = 3306

    # SharePoint設定
    site_url = "https://yourcompany.sharepoint.com/sites/yoursite"
    username = "your_email@yourcompany.com"
    password = "your_password"
    sharepoint_folder = "/sites/yoursite/Shared Documents/Folder"
    local_excel_path = "data_output.xlsx"

    # SSHトンネルセットアップ
    ssh_client, tunnel_redshift = setup_ssh_tunnel(
        ssh_host, ssh_port, ssh_user, ssh_key_path, local_port_redshift, remote_host_redshift, remote_port_redshift
    )
    ssh_client, tunnel_rds = setup_ssh_tunnel(
        ssh_host, ssh_port, ssh_user, ssh_key_path, local_port_rds, remote_host_rds, remote_port_rds
    )

    try:
        # ExcelからSQLを読み込み
        excel_path = "your_excel_file.xlsx"
        excel_data = pd.ExcelFile(excel_path)
        sql1 = excel_data.parse("sql1").iloc[0, 0]  # sql1シートの最初のセル
        sql2 = excel_data.parse("sql2").iloc[0, 0]  # sql2シートの最初のセル

        # Redshiftからデータ取得
        redshift_data = fetch_redshift_data(sql1)

        # RDSからデータ取得
        rds_data = fetch_rds_data(sql2)

        # データをExcelに保存
        with pd.ExcelWriter(local_excel_path, engine="openpyxl") as writer:
            redshift_data.to_excel(writer, sheet_name="data1", index=False)
            rds_data.to_excel(writer, sheet_name="data2", index=False)

        # SharePointにアップロード
        upload_to_sharepoint(site_url, username, password, local_excel_path, sharepoint_folder)

        # ファイル名変更
        old_file_name = "data_output.xlsx"
        new_file_name = "renamed_output.xlsx"
        rename_file_on_sharepoint(site_url, username, password, sharepoint_folder, old_file_name, new_file_name)

    finally:
        # SSH切断
        ssh_client.close()

if __name__ == "__main__":
    main()
