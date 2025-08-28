def hello_gcs(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file_name = event['name']
    bucket_name = event['bucket']

    print(f"File successfully uploaded.")


    print(f"File {file_name} uploaded to bucket {bucket_name}.")
