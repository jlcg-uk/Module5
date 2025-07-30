from flask import Flask, render_template, request

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    result = None
    error = None
    if request.method == "POST":
        try:
            num1 = float(request.form["num1"])
            num2 = float(request.form["num2"])
            operator = request.form["operator"]

            if operator == "+":
                result = num1 + num2
            elif operator == "-":
                result = num1 - num2
            elif operator == "*":
                result = num1 * num2
            elif operator == "/":
                if num2 == 0:
                    error = "Cannot divide by zero."
                else:
                    result = num1 / num2
            else:
                error = "Invalid operator selected."
        except ValueError:
            error = "Please enter valid numbers."
    return render_template("index.html", result=result, error=error)

if __name__ == "__main__":
    app.run(debug=True)
    