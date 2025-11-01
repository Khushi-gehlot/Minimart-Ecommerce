


from flask import Flask, render_template, request, jsonify
import databricks.sql as dbsql
from datetime import date

app = Flask(__name__)

# ---------------- Databricks connection ----------------
server_hostname = ""
http_path = ""
access_token = ""

# ---------------- Routes ----------------

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/cart')
def cart():
    return render_template('cart.html')
@app.route('/checkout')
def checkout():
    return render_template('checkout.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/wishlist')
def wishlist():
    return render_template('wishlist.html')


# ---------- Product Page ----------
# @app.route('/product_page/<int:product_id>')
# def product_page(product_id):
#     return render_template('product.html', product_id=product_id)
@app.route('/product_page/<int:product_id>')
def product_page(product_id):
    return render_template('product.html', product_id=product_id)


@app.route("/product/<int:product_id>")
def get_product(product_id):
    try:
        with dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT Products_id, Products_name, Brand, Price, Rating
                    FROM minimart_shared.default.products_deltatable
                    WHERE Products_id = {product_id}
                """)
                row = cursor.fetchone()
                if not row:
                    return jsonify({"error": "Product not found"}), 404
                columns = [desc[0] for desc in cursor.description]
                product = dict(zip(columns, row))
        return jsonify(product)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Render login page ----------
@app.route('/login', methods=['GET'])
def login_page():
    return render_template('login.html')

# ---------- LOGIN API ----------
@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")

    try:
        with dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT Customers_id, Customers_name 
                    FROM workspace.default.customers_data_minimart
                    WHERE Email = '{email}' AND Password = '{password}'
                """)
                user = cursor.fetchone()
                if user:
                    return jsonify({"success": True, "name": user[1]})
                else:
                    return jsonify({"success": False})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- SIGNUP API ----------
@app.route('/api/signup', methods=['POST'])
def api_signup():
    data = request.get_json()
    name = data.get("name")
    email = data.get("email")
    password = data.get("password")
    gender = data.get("gender")
    contact_no = data.get("contact_no")
    signup_date = date.today()
    status = "Active"

    try:
        with dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COALESCE(MAX(Customers_id), 0) + 1 FROM workspace.default.customers_data_minimart
                """)
                next_id = cursor.fetchone()[0]

                cursor.execute(f"""
                    INSERT INTO workspace.default.customers_data_minimart
                    (Customers_id, Customers_name, Email, Password, Gender, Contact_no, Signup_date, Status)
                    VALUES ({next_id}, '{name}', '{email}', '{password}', '{gender}', '{contact_no}', '{signup_date}', '{status}')
                """)
        return jsonify({"success": True, "message": "Signup successful!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
