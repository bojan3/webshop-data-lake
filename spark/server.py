import os
import subprocess
import traceback

from flask import Flask, jsonify, request

app = Flask(__name__)

SCRIPTS_DIR = "/flows"


@app.route("/run", methods=["POST"])
def run_script():
    filename = request.args.get("filename")

    if not filename:
        return jsonify(
            {"success": False, "message": "Filename parameter is required."}
        ), 400

    if not filename.endswith(".py"):
        return jsonify(
            {
                "success": False,
                "message": "Invalid file type. Only .py files are allowed.",
            }
        ), 400

    file_path = os.path.join(SCRIPTS_DIR, filename)

    if not os.path.isfile(file_path):
        return jsonify(
            {"success": False, "message": "File not found: {}".format(file_path)}
        ), 404

    try:
        process = subprocess.Popen(
            ["/spark/bin/spark-submit", file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = process.communicate()
        exit_code = process.returncode

        if exit_code == 0:
            return jsonify(
                {
                    "success": True,
                    "message": "Script executed successfully.",
                    "stdout": stdout,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "message": "Script execution failed.",
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code,
                }
            ), 500

    except Exception:
        return jsonify(
            {
                "success": False,
                "message": "Unexpected server error.",
                "traceback": traceback.format_exc(),
            }
        ), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
