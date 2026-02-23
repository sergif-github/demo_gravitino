# Base image: Jupyter con Spark (PySpark y R)
FROM jupyter/all-spark-notebook:latest

# Cambiamos a usuario root para poder instalar paquetes
USER root

# Instala Apache Toree a través de pip
RUN pip install toree

COPY mysql-connector-j-9.6.0.jar /usr/local/spark/jars/
COPY postgresql-42.7.10.jar /usr/local/spark/jars/
COPY gravitino-spark-connector-runtime-3.5_2.12-1.1.0.jar /usr/local/spark/jars/

# Regresa al usuario no privilegiado configurado en la imagen base
USER ${NB_UID}

# Instala el kernel de Scala para Apache Toree en el directorio de usuario
RUN jupyter toree install --spark_home=$SPARK_HOME --kernel_name=scala --user
