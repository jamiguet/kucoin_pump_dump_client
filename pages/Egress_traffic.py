import urllib.request

import streamlit as st

external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')

st.text(f"Egress IP: {external_ip}")
