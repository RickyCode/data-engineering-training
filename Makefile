COURSE_NAME = Data Engineerin Training
CLASS_THEME = Deployments

hello:
	echo "Hello world! Welcome to today class about $(CLASS_THEME) in our $(COURSE_NAME) course. Fasten your seatbelts and prepare to enjoy ${env}."


run-tests:
	cd  classes-content/class_2022_04_08_deployments/; pytest tests


send-file-to-aws:
	echo pending


deploy-lambda-funciton:
    

deploy-emr:
	aws s3 cd hfdakjslhf s3://hfalkjh/${env}/hjdklhas/hdakjlhsa
	emr
	spark-submit --file s3://hfalkjh/hfdalkjfshds
