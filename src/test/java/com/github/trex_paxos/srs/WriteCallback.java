package com.github.trex_paxos.srs;

import java.io.IOException;

public interface WriteCallback {

	void onWrite() throws IOException;

}
