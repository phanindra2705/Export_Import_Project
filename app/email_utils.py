from flask import request, jsonify
from flask_mail import Message
from __init__ import mail, app


def send_email(subject, recipient, body):
    msg = Message(subject, recipients=[recipient])
    msg.body = body.encode('latin-1', 'ignore').decode('latin-1')
    mail.send(msg)


@app.route('/send-email', methods=['POST'])
def send_email_api():
    data = request.get_json()

    subject = data.get('subject')
    recipient = data.get('recipient')
    body = data.get('body')

    if not subject or not recipient or not body:
        return jsonify({'error': 'Missing required parameters'}), 400

    try:
        send_email(subject, recipient, body)
        return jsonify({'message': 'Email sent successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500