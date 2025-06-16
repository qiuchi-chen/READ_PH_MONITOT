from flask import Flask, send_file
import os

app = Flask(__name__)

@app.route('/data')
def serve_csv():
    return send_file('processed_data.csv',
                    mimetype='text/csv',
                    as_attachment=False,
                    download_name='data.csv')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 