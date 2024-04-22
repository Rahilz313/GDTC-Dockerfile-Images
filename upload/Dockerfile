FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ${LAMBDA_TASK_ROOT}

COPY state_handler.py ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

CMD ["state_handler.lambda_handler"]
