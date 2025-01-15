from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

@app.route('/change_report')
def index():
    return render_template('change_report.html')


@app.route('/uploader', methods=['POST'])
def upload_file():
    uploaded_files = request.files.getlist('files')
    for file in uploaded_files:
        app.logger.info(file.filename)
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)