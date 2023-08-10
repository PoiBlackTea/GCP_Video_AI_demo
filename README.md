# GCP_Video_AI_demo

## DEMO目標
* 將 GCS 中的影片利用 Video AI API進行辨識將結果放到 BigQuery 內

1. 執行過程會先產生物件辨別資訊的Json檔放到 GCS內
2. 將 GCS 內 JSON檔 upload 到 BigQuery


## 執行步驟
使用 gcloud CLI
```
gcloud auth login
gcloud auth application-default login
```

Step 1. Git Clone project
```
git clone https://github.com/PoiBlackTea/GCP_Video_AI_demo.git
cd GCP_Video_AI_demo
```

Step 2. 設定環境變數
```
export PROJECT_ID="PROJECT_ID"
export DS_ID="BQ_DATASET_ID"
export TABLE_NAME="BQ_DATASET_TABLE"
export location="LOCATION"
export BUCKET_NAME="CLOUD_STORAGE_BUCKET"
export bucket_prefix="BUCKET_PATH"
```

Step 3. 建立 BigQuery Dataset 以及 Storage
```
bq --location=${location} mk \
    --dataset \
    ${PROJECT_ID}:${DS_ID}


gcloud storage buckets create gs://${BUCKET_NAME} --project=${PROJECT_ID} --location=${location}
```

Step 4. 上傳任意影片到 GCS Bucket 或者可以使用以下指令複製 GCP 公開影片到自己的GCS內
```
gsutil cp gs://cloud-samples-data/video/JaneGoodall.mp4 gs://${BUCKET_NAME}/${bucket_prefix}video1.mp4
gsutil cp gs://cloud-samples-data/video/JaneGoodall.mp4 gs://${BUCKET_NAME}/${bucket_prefix}video2.mp4
gsutil cp gs://cloud-samples-data/video/JaneGoodall.mp4 gs://${BUCKET_NAME}/${bucket_prefix}video3.mp4
```

Step 4. 環境準備
```
# 建立 python venv 
python -m venv <venv_name>
. <venv_name>/bin/activate

# pip install
pip install -r requirements.txt

# 更改程式執行使用的 .env 內容
vim .env
```

Step 5. 執行程式
```
python function_video_label.py
```

Step 6. 檢視 BigQuery 成果

上傳前的Json範例可以在 sample_labels folder查看