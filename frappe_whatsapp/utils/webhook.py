"""Webhook."""
import frappe
import json
import requests
from werkzeug.wrappers import Response
import frappe.utils

from frappe_whatsapp.utils import get_whatsapp_account


@frappe.whitelist(allow_guest=True)
def webhook():
	"""Meta webhook."""
	if frappe.request.method == "GET":
		return get()
	return post()


def get():
	"""Get."""
	hub_challenge = frappe.form_dict.get("hub.challenge")
	verify_token = frappe.form_dict.get("hub.verify_token")
	webhook_verify_token = frappe.db.get_value(
		'WhatsApp Account',
		{"webhook_verify_token": verify_token},
		'webhook_verify_token'
	)
	if not webhook_verify_token:
		frappe.throw("No matching WhatsApp account")

	if frappe.form_dict.get("hub.verify_token") != webhook_verify_token:
		frappe.throw("Verify token does not match")

	return Response(hub_challenge, status=200)


def post():
	"""Post."""
	data = frappe.local.form_dict
	
	# Ensure we have data - try parsing from request body if form_dict is empty
	if not data or not data.get("entry"):
		try:
			data = json.loads(frappe.request.data)
		except Exception:
			pass
	
	# Log the incoming webhook data first (commit immediately to ensure it's saved)
	try:
		frappe.get_doc({
			"doctype": "WhatsApp Notification Log",
			"template": "Webhook",
			"meta_data": json.dumps(data)
		}).insert(ignore_permissions=True)
		frappe.db.commit()
	except Exception as e:
		frappe.log_error(title="WhatsApp Webhook Log Error", message=f"Failed to log webhook: {str(e)}\nData: {json.dumps(data)}")

	messages = []
	phone_id = None
	try:
		messages = data["entry"][0]["changes"][0]["value"].get("messages", [])
		phone_id = data.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}).get("metadata", {}).get("phone_number_id")
	except (KeyError, TypeError, IndexError):
		# Fallback: try same structure again with safer access
		try:
			messages = data["entry"][0]["changes"][0]["value"].get("messages", [])
		except (KeyError, TypeError, IndexError):
			messages = []
	
	sender_profile_name = next(
		(
			contact.get("profile", {}).get("name")
			for entry in data.get("entry", [])
			for change in entry.get("changes", [])
			for contact in change.get("value", {}).get("contacts", [])
		),
		None,
	)

	whatsapp_account = get_whatsapp_account(phone_id) if phone_id else None
	if not whatsapp_account:
		frappe.log_error(title="WhatsApp Webhook - No Account", message=f"No WhatsApp account found for phone_id: {phone_id}")
		return Response("OK", status=200)

	if messages:
		for message in messages:
			try:
				process_single_message(message, sender_profile_name, whatsapp_account)
				frappe.db.commit()
			except Exception as e:
				message_type = message.get('type', 'unknown')
				frappe.log_error(
					title=f"WhatsApp Webhook - Message Processing Error ({message_type})",
					message=f"Error: {str(e)}\nMessage data: {json.dumps(message)}\nTraceback: {frappe.get_traceback()}"
				)
				frappe.db.rollback()
				continue
	else:
		changes = None
		try:
			changes = data["entry"][0]["changes"][0]
		except (KeyError, TypeError, IndexError):
			try:
				changes = data["entry"]["changes"][0]
			except Exception:
				changes = None
		if changes:
			update_status(changes)
	
	# Always return 200 OK to Meta - this is critical for webhook health
	return Response("OK", status=200)


def process_single_message(message, sender_profile_name, whatsapp_account):
	"""Process a single incoming WhatsApp message."""
	message_type = message.get('type', '')
	if not message_type:
		frappe.log_error(title="WhatsApp Webhook - No Message Type", message=f"Message has no type: {json.dumps(message)}")
		return
	
	meta_ts = get_message_timestamp(message)
	
	context = message.get('context', {})
	is_reply = bool(context and 'forwarded' not in context and context.get('id'))
	reply_to_message_id = context.get('id') if is_reply else None
	
	if message_type == 'text':
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message": message['text']['body'],
			"message_id": message['id'],
			"reply_to_message_id": reply_to_message_id,
			"is_reply": is_reply,
			"content_type": message_type,
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)
		
	elif message_type == 'reaction':
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message": message['reaction']['emoji'],
			"reply_to_message_id": message['reaction']['message_id'],
			"message_id": message['id'],
			"content_type": "reaction",
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)
		
	elif message_type == 'interactive':
		process_interactive_message(message, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts)
		
	elif message_type in ["image", "audio", "video", "document"]:
		process_media_message(message, message_type, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts)
		
	elif message_type == "button":
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message": message['button']['text'],
			"message_id": message['id'],
			"reply_to_message_id": reply_to_message_id,
			"is_reply": is_reply,
			"content_type": message_type,
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)
		
	elif message_type == "location":
		process_location_message(message, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts)
		
	elif message_type == "contacts":
		process_contacts_message(message, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts)
		
	else:
		# Handle unknown message types
		msg_content = ""
		if isinstance(message.get(message_type), dict):
			msg_content = message.get(message_type, {}).get(message_type, str(message.get(message_type, '')))
		else:
			msg_content = str(message.get(message_type, ''))
		
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message_id": message['id'],
			"message": msg_content,
			"content_type": message_type,
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)


def process_interactive_message(message, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts=None):
	"""Process interactive messages (buttons, lists, flows)."""
	interactive_data = message['interactive']
	interactive_type = interactive_data.get('type')

	if interactive_type == 'button_reply':
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message": interactive_data['button_reply']['id'],
			"message_id": message['id'],
			"reply_to_message_id": reply_to_message_id,
			"is_reply": is_reply,
			"content_type": "button",
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)
		
	elif interactive_type == 'list_reply':
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message": interactive_data['list_reply']['id'],
			"message_id": message['id'],
			"reply_to_message_id": reply_to_message_id,
			"is_reply": is_reply,
			"content_type": "button",
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)
		
	elif interactive_type == 'nfm_reply':
		nfm_reply = interactive_data['nfm_reply']
		response_json_str = nfm_reply.get('response_json', '{}')

		try:
			flow_response = json.loads(response_json_str)
		except json.JSONDecodeError:
			flow_response = {}

		summary_parts = []
		for key, value in flow_response.items():
			if value:
				summary_parts.append(f"{key}: {value}")
		summary_message = ", ".join(summary_parts) if summary_parts else "Flow completed"

		msg_doc = frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message": summary_message,
			"message_id": message['id'],
			"reply_to_message_id": reply_to_message_id,
			"is_reply": is_reply,
			"content_type": "flow",
			"flow_response": json.dumps(flow_response),
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)

		frappe.publish_realtime(
			"whatsapp_flow_response",
			{
				"phone": message['from'],
				"message_id": message['id'],
				"flow_response": flow_response,
				"whatsapp_account": whatsapp_account.name
			}
		)


def process_media_message(message, message_type, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts=None):
	"""Process media messages (image, audio, video, document)."""
	token = whatsapp_account.get_password("token")
	url = f"{whatsapp_account.url}/{whatsapp_account.version}/"

	media_id = message[message_type]["id"]
	headers = {
		'Authorization': 'Bearer ' + token
	}
	
	caption = message[message_type].get("caption", "")
	file_attached = False
	
	try:
		response = requests.get(f'{url}{media_id}/', headers=headers, timeout=30)

		if response.status_code == 200:
			media_data = response.json()
			media_url = media_data.get("url")
			mime_type = media_data.get("mime_type")
			file_extension = mime_type.split('/')[1] if mime_type and '/' in mime_type else 'bin'

			media_response = requests.get(media_url, headers=headers, timeout=60)
			if media_response.status_code == 200:
				file_data = media_response.content
				file_name = f"{frappe.generate_hash(length=10)}.{file_extension}"

				message_doc = frappe.get_doc(apply_meta_timestamp({
					"doctype": "WhatsApp Message",
					"type": "Incoming",
					"from": message['from'],
					"message_id": message['id'],
					"reply_to_message_id": reply_to_message_id,
					"is_reply": is_reply,
					"message": caption,
					"content_type": message_type,
					"profile_name": sender_profile_name,
					"whatsapp_account": whatsapp_account.name
				}, meta_ts)).insert(ignore_permissions=True)

				file = frappe.get_doc({
					"doctype": "File",
					"file_name": file_name,
					"attached_to_doctype": "WhatsApp Message",
					"attached_to_name": message_doc.name,
					"content": file_data,
					"attached_to_field": "attach"
				}).save(ignore_permissions=True)

				message_doc.attach = file.file_url
				message_doc.save()
				file_attached = True
			else:
				frappe.log_error(
					title="WhatsApp Media Content Download Error",
					message=f"Failed to download media content: {media_response.status_code}"
				)
		else:
			frappe.log_error(
				title="WhatsApp Media URL Error",
				message=f"Failed to get media URL: {response.status_code} - {response.text[:200]}"
			)
	except requests.exceptions.Timeout:
		frappe.log_error(title="WhatsApp Media Timeout", message=f"Timeout downloading media: {media_id}")
	except Exception as e:
		frappe.log_error(title="WhatsApp Media Error", message=f"Error downloading media: {str(e)}")
	
	# Create message even if media download failed
	if not file_attached:
		frappe.get_doc(apply_meta_timestamp({
			"doctype": "WhatsApp Message",
			"type": "Incoming",
			"from": message['from'],
			"message_id": message['id'],
			"reply_to_message_id": reply_to_message_id,
			"is_reply": is_reply,
			"message": caption or f"[{message_type} - download failed]",
			"content_type": message_type,
			"profile_name": sender_profile_name,
			"whatsapp_account": whatsapp_account.name
		}, meta_ts)).insert(ignore_permissions=True)


def process_location_message(message, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts=None):
	"""Process location messages."""
	location_data = message.get('location', {})
	
	latitude = location_data.get('latitude')
	longitude = location_data.get('longitude')
	name = location_data.get('name', '')
	address = location_data.get('address', '')
	
	# Build Google Maps URL if coordinates are available
	maps_url = ""
	if latitude is not None and longitude is not None:
		maps_url = f"https://maps.google.com/?q={latitude},{longitude}"
	
	location_json = json.dumps({
		"latitude": latitude,
		"longitude": longitude,
		"name": name,
		"address": address,
		"url": maps_url
	})
	
	frappe.get_doc(apply_meta_timestamp({
		"doctype": "WhatsApp Message",
		"type": "Incoming",
		"from": message['from'],
		"message": location_json,
		"message_id": message['id'],
		"reply_to_message_id": reply_to_message_id,
		"is_reply": is_reply,
		"content_type": "location",
		"profile_name": sender_profile_name,
		"whatsapp_account": whatsapp_account.name
	}, meta_ts)).insert(ignore_permissions=True)


def process_contacts_message(message, sender_profile_name, whatsapp_account, is_reply, reply_to_message_id, meta_ts=None):
	"""Process contacts messages."""
	contacts_data = message.get('contacts', [])
	
	# Create a summary of contacts
	contacts_summary = []
	for contact in contacts_data:
		contact_name = contact.get('name', {})
		formatted_name = contact_name.get('formatted_name', '')
		phones = contact.get('phones', [])
		phone_numbers = [p.get('phone', '') for p in phones]
		contacts_summary.append({
			"name": formatted_name,
			"phones": phone_numbers
		})
	
	contacts_json = json.dumps(contacts_summary)
	
	frappe.get_doc(apply_meta_timestamp({
		"doctype": "WhatsApp Message",
		"type": "Incoming",
		"from": message['from'],
		"message": contacts_json,
		"message_id": message['id'],
		"reply_to_message_id": reply_to_message_id,
		"is_reply": is_reply,
		"content_type": "contact",
		"profile_name": sender_profile_name,
		"whatsapp_account": whatsapp_account.name
	}, meta_ts)).insert(ignore_permissions=True)


def update_status(data):
	"""Update status hook."""
	if data.get("field") == "message_template_status_update":
		update_template_status(data['value'])
	elif data.get("field") == "messages":
		update_message_status(data['value'])


def update_template_status(data):
	"""Update template status."""
	frappe.db.sql(
		"""UPDATE `tabWhatsApp Templates`
		SET status = %(event)s
		WHERE id = %(message_template_id)s""",
		data
	)


def update_message_status(data):
	"""Update message status."""
	try:
		id = data['statuses'][0]['id']
		status = data['statuses'][0]['status']
		conversation = data['statuses'][0].get('conversation', {}).get('id')
		name = frappe.db.get_value("WhatsApp Message", filters={"message_id": id})

		if name:
			doc = frappe.get_doc("WhatsApp Message", name)
			doc.status = status
			if conversation:
				doc.conversation_id = conversation
			doc.save(ignore_permissions=True)
	except Exception as e:
		frappe.log_error(title="WhatsApp Status Update Error", message=f"Error: {str(e)}\nData: {json.dumps(data)}")


def get_message_timestamp(message):
	"""Convert Meta timestamp (seconds since epoch, UTC) to datetime."""
	ts = message.get("timestamp")
	if not ts:
		return None
	try:
		return frappe.utils.datetime.datetime.fromtimestamp(int(ts))
	except Exception:
		return None


def apply_meta_timestamp(doc_dict, meta_ts=None):
	"""Attach meta timestamp to creation/modified if available."""
	if meta_ts:
		doc_dict["creation"] = meta_ts
		doc_dict["modified"] = meta_ts
	return doc_dict
