{\rtf1\ansi\ansicpg1252\cocoartf2761
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 #!/bin/bash\
\
# Set the Google Cloud project\
gcloud config set project ba882-group-10\
\
# Census API function deployment\
echo "======================================================"\
echo "Deploying the Census API function"\
echo "======================================================"\
\
gcloud functions deploy census-api \\\
    --gen2 \\\
    --runtime python311 \\\
    --trigger-http \\\
    --entry-point census_api \\  # Replace with the actual entry point function name\
    --source ./functions/census-api \\  # Path to your function's source code\
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \\\
    --region us-central1 \\\
    --allow-unauthenticated \\\
    --memory 2048MB \\\
    --timeout 600s}