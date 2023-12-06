prefect deployment apply prefect_deploy/init_db_flow-docker-deployment.yaml
prefect deployment apply prefect_deploy/extract_file_flow-docker-deployment.yaml
prefect deployment apply prefect_deploy/load_data_flow-docker-deployment.yaml
prefect agent start -q 'iot-demo-agent'
