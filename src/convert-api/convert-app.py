from flask import Flask, request, jsonify

app = Flask(__name__)

# The function to convert Celsius to Fahrenheit
def celsius_to_fahrenheit(celsius):
    return celsius * 9./5 + 32

@app.route('/convert', methods=['POST'])
def convert():
    # Get JSON data from request
    data = request.get_json()

    # Check if the 'data' key exists in the received JSON
    if 'data' not in data:
        return jsonify({'error': 'Missing data key in request'}), 400

    # Extract the 'data' list from the received JSON
    data_list = data['data']

    # Initialize a list to store converted values
    converted_data = []

    # Iterate over each item in 'data_list'
    for item in data_list:
        # Check if the item is a list with at least two elements
        if not isinstance(item, list) or len(item) < 2:
            return jsonify({'error': 'Invalid data format'}), 400
        
        # Extract the Celsius value
        celsius = item[1]
        
        # Convert to Fahrenheit and append to 'converted_data'
        converted_data.append([item[0], celsius_to_fahrenheit(celsius)])

    # Return the converted data as JSON
    return jsonify({'data': converted_data})

if __name__ == '__main__':
    app.run(debug=True)
