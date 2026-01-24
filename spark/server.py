from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

SCRIPTS_DIR = "./scripts"

@app.route("/run", methods=["POST"])
def run_script():
    filename = request.args.get("filename")

    if not filename:
        return jsonify({
            "success": False,
            "message": "Filename parameter is required."
        }), 400

    file_path = os.path.join(SCRIPTS_DIR, filename)

    if not filename.endswith(".py"):
        return jsonify({
            "success": False,
            "message": "Invalid file type. Only .py files are allowed."
        })

    if not os.path.isfile(file_path):
        return jsonify({
            "success": False,
            "message": "File not found."
        })

    try:
        subprocess.run(
            ["python", file_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )

        return jsonify({
            "success": True,
            "message": "Script executed successfully."
        })

    except:
        return jsonify({
            "success": False,
            "message": "Script execution failed."
        })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
