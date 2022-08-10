DATA_LAKE_ACCOUNT=XXXXXXXXXXXX
DATA_LAKE_PROFILE=datalake account profile
DATA_PRODUCER_ACCOUNT=XXXXXXXXXXXX
DATA_PRODUCER_PROFILE=producer account profile

REGION=us-east-1
.DEFAULT_GOAL := deploy-all
export CDK_DEFAULT_REGION=$(REGION)

set-datalake-policy:
	@echo "Set Data Lake Glue policy"
	python3 scripts/set_glue_policy.py --region $(REGION) --account $(DATA_LAKE_ACCOUNT) --profile $(DATA_LAKE_PROFILE) --consumer $(DATA_LAKE_ACCOUNT)
	@echo "Set Data Lake Glue policy Done."

#set-data-consumer-policy:
#	@echo "Set Data Consumer Glue policy"
#	python scripts/set_glue_policy_receiver.py --region $(REGION) --account $(DATA_LAKE_ACCOUNT) --profile $(DATA_LAKE_PROFILE)
#	@echo "Set Data Consumer Glue policy done"

deploy-data-producer:
	@echo "Deploying Data Producer"
	cdk synth -c config=DataProducer -c region=$(REGION) --profile $(DATA_PRODUCER_PROFILE)
	cdk deploy -c config=DataProducer -c region=$(REGION) --profile $(DATA_PRODUCER_PROFILE)
	@echo "Deploying Data Producer done"

deploy-datalake:
	@echo "Deploying Data Lake"
	cdk synth -c config=DataLake -c region=$(REGION) --profile $(DATA_LAKE_PROFILE)
	cdk deploy -c config=DataLake -c region=$(REGION) --profile $(DATA_LAKE_PROFILE)
	@echo "Deploying Data Lake done"

destroy-data-producer:
	@echo "Destroy Data Producer"
	cdk destroy -c config=DataProducer -c region=$(REGION) --profile $(DATA_PRODUCER_PROFILE)
	@echo "Destroy Data Producer done"

destroy-datalake:
	@echo "Destroy Data Lake"
	cdk destroy -c config=DataLake -c region=$(REGION) --profile $(DATA_LAKE_PROFILE)
	@echo "Destroy Data Lake done"

bootstrap-producer:
	@echo "Running CDK Bootstrap on Producer Account"
	cdk bootstrap -c config=DataProducer -c region=$(REGION) --qualifier=mc-stack --profile $(DATA_PRODUCER_PROFILE) --force

bootstrap-datalake:
	@echo "Running CDK Bootstrap on Datalake Account"
	cdk  bootstrap -c config=DataLake -c region=$(REGION) --qualifier=mc-stack --profile $(DATA_LAKE_PROFILE) --force


bootstrap: bootstrap-producer bootstrap-datalake

destroy-all: destroy-data-producer destroy-datalake

help:
	@echo "Please use \`make <target>' where <target> is one of"
