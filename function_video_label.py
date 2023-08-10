from io import StringIO
from os import getenv
from typing import Optional, Sequence, cast

from dotenv import load_dotenv
from google.cloud import bigquery, storage
from google.cloud import videointelligence_v1 as vi


def detect_labels(
    video_uri: str,
    mode: vi.LabelDetectionMode,
    segments: Optional[Sequence[vi.VideoSegment]] = None,
) -> vi.VideoAnnotationResults:
    video_client = vi.VideoIntelligenceServiceClient()
    features = [vi.Feature.LABEL_DETECTION]
    config = vi.LabelDetectionConfig(label_detection_mode=mode)
    context = vi.VideoContext(segments=segments, label_detection_config=config)
    request = vi.AnnotateVideoRequest(
        input_uri=video_uri,
        features=features,
        video_context=context,
    )

    print(f'Processing video "{video_uri}"...')
    operation = video_client.annotate_video(request)

    # Wait for operation to complete
    response = cast(vi.AnnotateVideoResponse, operation.result())
    # A single video is processed
    results = response.annotation_results[0]

    return results


def category_entities_to_str(
    category_entities: Sequence[vi.Entity]
) -> str:
    if not category_entities:
        return ""
    entities = ", ".join([e.description for e in category_entities])
    return f" ({entities})"


def sorted_by_first_segment_start_and_confidence(
    labels: Sequence[vi.LabelAnnotation],
) -> Sequence[vi.LabelAnnotation]:
    def first_segment_start_and_confidence(label: vi.LabelAnnotation):
        first_segment = label.segments[0]
        ms = first_segment.segment.start_time_offset.total_seconds()
        return (ms, -first_segment.confidence)

    return sorted(labels, key=first_segment_start_and_confidence)


def video_shot_labels(
    video_uri: str,
    results: vi.VideoAnnotationResults
) -> str:
    labels = sorted_by_first_segment_start_and_confidence(
        results.shot_label_annotations
    )
        
    # with open("labels.json", "w", newline="")as jsonfile:
    with StringIO() as jsonfile:
        for label in labels:
            for segment in label.segments:
                tmp = {
                    "file_name": video_uri,
                    "entity": label.entity.description,
                    "categories": category_entities_to_str(label.category_entities),
                    "confidence": f"{segment.confidence:4.0%}",
                    "start time": f"{segment.segment.start_time_offset.total_seconds():7.3f}",
                    "end time": f"{segment.segment.end_time_offset.total_seconds():7.3f}"
                }
                jsonfile.write(f"{str(tmp)}\n")
        
        contents = jsonfile.getvalue()
    return contents


def upload_blob_from_memory(
    bucket_name: str, 
    contents: str, 
    destination_blob_name: str
) -> None:
    """Uploads a file to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)


def upload_json_from_cloud_storage(
    source_blob_path: str, 
    bigquery_table_id: str, 
    bigquery_location: str
) -> None:
    """Uploads a json format file to the BigQury table"""
    # # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED"
    )
    
    load_job = client.load_table_from_uri(
        source_uris=source_blob_path,
        destination=bigquery_table_id,
        location=bigquery_location,  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.


if __name__=='__main__':

    try:
        load_dotenv()
        backet_name = getenv('backet_name')
        bigquery_table_id = getenv('bigquery_table_id')
        bigquery_location = getenv('bigquery_location')
        video_prefix_bucket = getenv('video_prefix_bucket')

        
        storage_client = storage.Client()
        bucket = storage_client.bucket(backet_name)
        blobs = bucket.list_blobs(delimiter='/', prefix=video_prefix_bucket)
        for blob in blobs:
            video_uri = f"gs://{backet_name}/{blob.name}"

            results = detect_labels(video_uri, vi.LabelDetectionMode.SHOT_MODE)
            contents = video_shot_labels(video_uri, results)
            destination_blob_name = f"{blob.name.replace(video_prefix_bucket, '')}.json"

            upload_blob_from_memory(backet_name, contents, destination_blob_name)
            upload_json_from_cloud_storage(f"gs://{backet_name}/{destination_blob_name}", bigquery_table_id, bigquery_location)
    except Exception as e:
        print(e)

# storage video -> cloud run/function -> video api -> memory file -> cloud storage -> import json to BQ