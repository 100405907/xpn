package org.expand;

import java.nio.ByteBuffer;

import org.expand.jni.ExpandToPosix;
import org.expand.jni.Stat;
import org.expand.jni.ExpandFlags;

public class testExpandToPosix {
	
	public static void main(String[] args) {

		ExpandToPosix xpn = new ExpandToPosix();
		byte b[] = new byte[65536];
		ByteBuffer buf = ByteBuffer.allocateDirect(b.length);

		try {
			xpn.jni_xpn_init();
			int fd = xpn.jni_xpn_open("/xpn/newtestwrfile", xpn.flags.O_RDWR);
			System.out.println("FD OPEN: " + fd);
			System.out.println("BYTES LEIDOS: " + xpn.jni_xpn_read(fd, buf, 65536));
			xpn.jni_xpn_close(fd);
			
			xpn.jni_xpn_destroy();
		}catch (Exception e){
			System.out.println("EXCEPCION: " + e);
		}
	}
}
