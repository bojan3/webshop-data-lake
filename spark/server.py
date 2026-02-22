import os
import subprocess
import traceback

from flask import Flask, jsonify, request

app = Flask(__name__)

SCRIPTS_DIR = "/flows"


@app.route("/run", methods=["POST"])
def run_script():
    filename = request.args.get("filename")
    data_period = (
        request.args.get("data_period")
        or request.args.get("data-period")
        or request.form.get("data_period")
        or request.form.get("data-period")
        or (request.get_json(silent=True) or {}).get("data_period")
        or (request.get_json(silent=True) or {}).get("data-period")
    )

    if not filename:
        return jsonify(
            {"success": False, "message": "Filename parameter is required."}
        ), 400

    if not data_period:
        return jsonify(
            {
                "success": False,
                "message": "Data period is required. Provide data_period (or data-period).",
            }
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
        cmd = ["/spark/bin/spark-submit", file_path]
        if data_period:
            cmd.extend(["--data-period", data_period])

        process = subprocess.Popen(
            cmd,
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
                    "command": cmd,
                    "data_period": data_period,
                    "stdout": stdout,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "message": "Script execution failed.",
                    "command": cmd,
                    "data_period": data_period,
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
