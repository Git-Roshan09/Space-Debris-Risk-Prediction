#!/usr/bin/env python3
"""Simple HTTP server to trigger Spark job submissions."""

from http.server import HTTPServer, BaseHTTPRequestHandler
import subprocess
import json

class SparkTriggerHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/submit':
            try:
                cmd = [
                    '/opt/spark/bin/spark-submit',
                    '--master', 'spark://spark-master:7077',
                    '--deploy-mode', 'client',
                    '--name', 'SGP4-Vector-Computation',
                    '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
                    '--conf', 'spark.executor.memory=2g',
                    '--conf', 'spark.executor.cores=2',
                    '--conf', 'spark.driver.memory=1g',
                    '/opt/spark-apps/processing/spark_sgp4_to_hdfs.py'
                ]
                
                # Start job in background
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = json.dumps({
                    'success': True,
                    'message': 'Spark job submitted',
                    'pid': process.pid
                })
                self.wfile.write(response.encode())
                
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = json.dumps({
                    'success': False,
                    'error': str(e)
                })
                self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 6066), SparkTriggerHandler)
    print('Spark trigger server listening on port 6066...')
    server.serve_forever()
