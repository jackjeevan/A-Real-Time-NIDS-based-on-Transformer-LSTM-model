from scapy.sendrecv import AsyncSniffer
from flow_session import generate_session_class

def createSniffer(input_file, input_interface, output_mode, output_file, label, url_model=None):
    assert (input_file is None) ^ (input_interface is None)
    
    NewFlowSession = generate_session_class(output_mode, output_file, label, url_model)

    if input_file is not None:
        return AsyncSniffer(
            offline=input_file,
            filter="ip and (tcp or udp)",
            prn=None,
            session=NewFlowSession,
            store=False,
        )
    else:
        return AsyncSniffer(
            iface=input_interface,
            filter="ip and (tcp or udp)",
            prn=None,
            session=NewFlowSession,
            store=False,
        )
