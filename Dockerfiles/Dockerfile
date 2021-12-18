FROM jupyter/datascience-notebook:latest
RUN pip3 install psycopg2-binary
USER root
RUN mkdir /home/task1
RUN mkdir /home/task1/scripts
RUN mkdir /home/task1/files
RUN mkdir /home/task2
RUN mkdir /home/task2/scripts
RUN mkdir /home/task2/files

CMD /home
# ENTRYPOINT (cd /home) && (nohup jupyter notebook --port=8888 --no-browser --ip="*" --allow-root --NotebookApp.token='pass'  --NotebookApp.password='' &> nohup_jupyter.out &) && (sleep infinity)

ENTRYPOINT (cd /home) && (jupyter notebook --port=8888 --no-browser --ip="*" --allow-root --NotebookApp.token='pass'  --NotebookApp.password='' --NotebookApp.notebook_dir='/home/')