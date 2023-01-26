zip lambda_function.zip ffmpeg_lambda.py
aws lambda update-function-code --function-name videocloud-ffmpeg --zip-file fileb://lambda_function.zip 
rm lambda_function.zip 
