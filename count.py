import csv
import re

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
            from_addr = row[3]
            to_addr = row[4]
            
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
            # 8列目（インデックス7）が1のデータをカウント
            if row[7] == '1' or row[6] == '1':
                contract_address.add(to_addr)
                sctx_count += 1
                continue

            # 正常なアドレスのみユニークセットに追加
            unique_account.add(from_addr)
            unique_account.add(to_addr)

    print("===================tx==================")
    print(f"Total Rows: {total_rows_count}")
    print(f"Correct Data: {correct_data_count}")
    print(f"Invalid Data Count: {invalid_data_count}")
    print(f"Unique Account: {len(unique_account)}")
    print(f"Contract Address: {len(contract_address)}")
    print(f"SCTX Count: {sctx_count}")
    print(f"Normal tx: {correct_data_count - sctx_count}")
    print("=====================================")

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

# 関数を実行する例
csv_path = 'selectedTxs_1000K.csv'
csv_path_itx = 'selectedInternalTxs_1000K.csv'
print_tx_info(csv_path, 50000)
print_itx_info(csv_path_itx, 280742)