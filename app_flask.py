from flask import Flask, request, jsonify

app = Flask(__name__)

wallets = []


@app.route('/wallets/new', methods=['POST'])
def update_wallets():
    global wallets
    new_wallets = request.json.get('wallets')
    if new_wallets:
        wallets_to_add = [wallet for wallet in new_wallets if wallet not in wallets]
        wallets.extend(wallets_to_add)
        return jsonify({'message': 'Wallets updated successfully', 'wallets': wallets})
    else:
        return jsonify({'message': 'No wallets provided'}), 400


@app.route('/wallets', methods=['GET'])
def get_wallets():
    return jsonify(wallets)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=666)