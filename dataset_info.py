import csv
import re
import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt


# アドレスの形式が正しいかどうかをチェックする関数
def is_valid_address(address):
    # アドレスが "0x" で始まり、その後に40文字の16進数が続くか確認
    if re.match(r'^0x[a-fA-F0-9]{40}$', address):
        return True
    return False

def print_tx_info(csv_path, max_rows=None):
    total_rows_count = 0
    correct_data_count = 0
    sctx_count = 0
    invalid_data_count = 0
    contract_address = set()
    unique_account = set()
    address_frequency = defaultdict(int)  # アドレス出現頻度カウンタ追加

    # CSVファイルを開く
    with open(csv_path, mode='r') as file:
        reader = csv.reader(file)

        # 各行をループ
        for row in reader:
            # 指定された行数に達したら終了
            if max_rows is not None and total_rows_count >= max_rows:
                break
            
            total_rows_count += 1
            
            # 3列目（インデックス3）と4列目（インデックス4）のアドレスを取得
            from_addr = row[3]
            to_addr = row[4]
            
            # fromIsContract, toIsContractを表す列を想定（例えば6列目、7列目あたり）
            # ここではデータ例から row[6], row[7] がfromIsContract, toIsContractとして扱う
            if len(row) > 7:
                from_is_contract = (row[6] == '1')
                to_is_contract = (row[7] == '1')
            else:
                # CSVフォーマットが異なる場合は適宜対応
                from_is_contract = False
                to_is_contract = False

            # アドレスの正当性をチェック
            if not is_valid_address(from_addr):
                invalid_data_count += 1
                continue
            if not is_valid_address(to_addr):
                invalid_data_count += 1
                continue
            if from_addr == to_addr:
                invalid_data_count += 1
                continue

            correct_data_count += 1
            # コントラクトが含まれるトランザクションの場合
            if from_is_contract or to_is_contract:
                contract_address.add(to_addr)
                sctx_count += 1
                continue
            else:
                # 両方非コントラクトの場合のみカウント
                unique_account.add(from_addr)
                unique_account.add(to_addr)
                address_frequency[from_addr] += 1
                address_frequency[to_addr] += 1

    print("===================tx==================")
    print(f"Total Rows: {total_rows_count}")
    print(f"Correct Data: {correct_data_count}")
    print(f"Invalid Data Count: {invalid_data_count}")
    print(f"Unique Account: {len(unique_account)}")
    print(f"Contract Address: {len(contract_address)}")
    print(f"SCTX Count: {sctx_count}")
    print(f"Normal tx: {correct_data_count - sctx_count}")
    print("=====================================")

    # address_frequencyを出現回数降順でソート
    sorted_addresses = sorted(address_frequency.items(), key=lambda x: x[1], reverse=True)
    top_500 = sorted_addresses[:100]

    print("=== Top 100 Non-Contract Addresses by Frequency ===")
    for addr, freq in top_500:
        # print(addr, freq)
        print(addr)
    print("===================================================")

def print_itx_info(csv_path, max_rows=None):
    total_rows_count = 0
    correct_data_count = 0
    sctx_count = 0
    invalid_data_count = 0
    contract_address = set()
    account_address = set()
    unique_itx_count = set()
    # CSVファイルを開く
    with open(csv_path, mode='r') as file:
        reader = csv.reader(file)

        # 各行をループ
        for row in reader:
            # 指定された行数に達したら終了
            if max_rows is not None and total_rows_count >= max_rows:
                break
            total_rows_count += 1
            
            # 3列目（インデックス2）と4列目（インデックス3）のアドレスを取得
            parent_tx_hash = row[2]
            from_addr = row[4]
            to_addr = row[5]
            fromIsContract = row[6] == '1'
            toIsContract = row[7] == '1'
            
            # アドレスの正当性をチェック
            if not is_valid_address(from_addr):
                invalid_data_count += 1
                continue
            if not is_valid_address(to_addr):
                invalid_data_count += 1
                continue

            correct_data_count += 1

            if fromIsContract:
                contract_address.add(from_addr)
            else:
                account_address.add(from_addr)
            if toIsContract:
                contract_address.add(to_addr)
            else:
                account_address.add(to_addr)

            # 正常なアドレスのみユニークセットに追加
            unique_itx_count.add(parent_tx_hash)
    
    print("==================itx===================")
    print(f"Total Rows: {total_rows_count}")
    print(f"Correct Data: {correct_data_count}")
    print(f"Invalid Data Count: {invalid_data_count}")
    print(f"Unique ITx Count: {len(unique_itx_count)}")
    print(f"Contract Address: {len(contract_address)}")
    print(f"Account Address: {len(account_address)}")
    # print(f"SCTX Count: {sctx_count}")
    # print(f"Normal tx: {correct_data_count - sctx_count}")
    print("=====================================")

def remove_columns_and_keep_comma(input_file, output_file, columns_to_remove):
    """
    指定した列を削除し、その位置に空のカンマを保持したままCSVを保存する関数。
    
    Parameters:
    input_file (str): 元のCSVファイルのパス
    output_file (str): 結果を保存するCSVファイルのパス
    columns_to_remove (list): 削除したい列番号(0ベースインデックス)のリスト
    """
    # 元のCSVファイルを読み込む (header=None でヘッダーなしとして読み込む)
    df = pd.read_csv(input_file, header=None)
    
    # 指定した列を削除し、空の列をその位置に追加
    for col in columns_to_remove:
        df[col] = ''  # 指定した列に空の文字列をセット
    
    # 結果を新しいCSVファイルとして保存する (カンマ区切りはそのまま保持)
    df.to_csv(output_file, header=False, index=False)

# 関数を実行する例
csv_path = '/app/block-emulator/dataset/20000000to20249999_BlockTransaction_2M.csv'
# csv_path_itx = 'selectedInternalTxs_1000K.csv'
print_tx_info(csv_path)
# print_itx_info(csv_path_itx, 280742)

def load_internal_txs_from_csv(csv_path):
    internal_tx_map = defaultdict(int)
    with open(csv_path, mode='r') as file:
        reader = csv.reader(file)
        # Loop through each row
        for row in reader:
            # Get the address from the 3rd column (index 2)
            parent_tx_hash = row[2]
            internal_tx_map[parent_tx_hash] += 1

    # Calculate the average
    if len(internal_tx_map) > 0:
        average_internal_tx = sum(internal_tx_map.values()) / len(internal_tx_map)
    else:
        average_internal_tx = 0
    print(f"Average internal transaction count: {average_internal_tx}")

    # Group data into 10-count intervals and display in the terminal
    range_counts = defaultdict(int)
    for count in internal_tx_map.values():
        # Find the appropriate range for each count
        range_key = (count // 10) * 10
        range_counts[range_key] += 1

    # Print the results in 10-count intervals
    print("Internal transaction counts by 10-count intervals:")
    for range_start in sorted(range_counts.keys()):
        range_end = range_start + 9
        print(f"{range_start} - {range_end}: {range_counts[range_start]} occurrences")
        
# input_file = 'selectedInternalTxs_1000K.csv'  # 元のCSVファイル
# output_file = 'selectedInternalTxs_1000K_light.csv'  # 変換後のCSVファイル
# columns_to_remove = [0, 1, 9, 10]
# remove_columns_and_keep_comma(input_file, output_file, columns_to_remove)

# csv_path = './dataset/selectedInternalTxs_1000K.csv'
# load_internal_txs_from_csv(csv_path)