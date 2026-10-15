#!/bin/sh 
# Eksekusi deklarasi topologi DLX via RabbitMQ CLI di dalam kontainer

docker exec medcore-message-broker rabbitmqadmin -u admin_medcore -p 
SecuredPassRabbit2026! -V /medcore_vhost declare exchange 
name=medcore.deadletter.exchange type=fanout durable=true 

docker exec medcore-message-broker rabbitmqadmin -u admin_medcore -p 
SecuredPassRabbit2026! -V /medcore_vhost declare queue name=pharmacy.dlq 
durable=true 

docker exec medcore-message-broker rabbitmqadmin -u admin_medcore -p 
SecuredPassRabbit2026! -V /medcore_vhost declare binding 
source=medcore.deadletter.exchange destination=pharmacy.dlq