package org.expand;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Progressable;

import org.expand.ExpandFSInputStream;
import org.expand.ExpandToPosix;
import org.expand.ExpandFlags;
import org.expand.ExpandOutputStream;
import org.expand.Stat;

public class Expand extends FileSystem {

	private ExpandToPosix xpn;
	private URI uri;
	private Path workingDirectory;
	public ExpandFlags flags;
	private long blksize = 524288;
	private int bufsize = 65536;
	private boolean initialized;

	public Expand(){
		System.out.println("ENTRO AL CONSTRUCTOR");
		this.xpn = new ExpandToPosix();
		System.out.println("DESPUES DE XPN INIT");
		this.uri = URI.create("xpn:///");
		this.setWorkingDirectory(new Path("/xpn"));
		this.flags = this.xpn.flags;
		this.initialized = false;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		System.out.println("------------------ENTRO A INITIALIZE------------------");
		try{
		//if (this.initialized) return;
		super.initialize(getUri(), conf);
		this.xpn.jni_xpn_init();
		this.initialized = true;
		}catch(Exception e){
			System.out.println("Excepcion en INITIALIZE: " + e);
		return;
		}
		System.out.println("------------------SALGO DE INITIALIZE------------------");
	}

	public void close() throws IOException {
		System.out.println("------------------ENTRO A CLOSE------------------");
		this.initialized = false;
		// this.xpn.jni_xpn_destroy();
		// super.close();
		System.out.println("------------------SALGO DE CLOSE------------------");
	}

	public void loadFileToExpand(Configuration conf, Path src, Path dst) throws IOException {
    	FSDataInputStream is = null;
    	FSDataOutputStream os = null;
		try {
			FileSystem fs = src.getFileSystem(conf);
			is = fs.open(src, 4096);
			os = create(dst, FsPermission.getFileDefault(), true, 4096, (short) 0, (long) 4096, null);
			byte[] buffer = new byte[1024];
			int length;
			while ((length = is.read(buffer)) > 0) {
				os.write(buffer, 0, length);
			}
		} finally {
			is.close();
			os.close();
		}
  	}

	@Override
	public FileStatus getFileStatus (Path path){
		System.out.println("---------------------ENTRO A GETFILESTATUS-----------------");
		path = removeURI(path);
		System.out.println("PATH QUE ENTRA: " + path.toString());
		
		if (!exists(path)) return null;
		
		Stat stats = this.xpn.jni_xpn_stat(path.toString());

		boolean isdir = this.xpn.jni_xpn_isDir(stats.st_mode) != 0;
		String username = this.xpn.jni_xpn_getUsername((int) stats.st_uid);
		String groupname = this.xpn.jni_xpn_getGroupname((int) stats.st_gid);
		FsPermission permission = new FsPermission(Integer.toOctalString(stats.st_mode & 0777));
		System.out.println("-------------------SALGO DE GETFILESTATUS--------------------------");

		return new FileStatus(stats.st_size, isdir, 0, stats.st_blksize,
			       stats.st_mtime * 1000, stats.st_atime * 1000, 
			       permission, username, groupname, path);
	}

	@Override
    	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		System.out.println("------------------ENTRO A MKDIRS------------------");
		path = removeURI(path);
		
		//short mode = permission.toShort();
		//FsPermission perm = new FsPermission(mode);
		path = removeURI(path);
		String relPath = "";
		String absPath;

		String [] dirs = path.toString().split("/");

		for (int i = 2; i < dirs.length; i++){
			relPath += "/" + dirs[i];
			if (exists(new Path(relPath))) continue;
			absPath = makeAbsolute(new Path (relPath)).toString();
			System.out.println("ITERACION: " + i + " PATH: " + absPath);
			System.out.println("Perm.toshort()= " + permission.toShort());
			int res = this.xpn.jni_xpn_mkdir(absPath , permission.toShort());
			if (res != 0) {
				return false;
			}
		}

		System.out.println("------------------SALGO DE MKDIRS------------------");

		return true;
	}

	@Override
    	public Path getWorkingDirectory() {
        	return this.workingDirectory;
    	}

	@Override
    	public void setWorkingDirectory(Path new_dir) {
		new_dir = removeURI(new_dir);
        	this.workingDirectory = new_dir;
    	}

	@Override
    	public FileStatus[] listStatus(Path f){
		System.out.println("----------------------ENTRO A LISTSTATUS---------------------");
		f = removeURI(f);
		
		if (!exists(f))
			return null;
		
		if (!isDir(f)){
			FileStatus [] list = new FileStatus[1];
			list[0] = getFileStatus(f);
			return list;
		}
		
		String str [] = this.xpn.jni_xpn_getDirContent(f.toString());
		FileStatus list [] = new FileStatus [str.length - 2];
		for (int i = 0; i < list.length; i++){
			list[i] = getFileStatus(new Path(f.toString() + "/" + str[i + 2]));
			System.out.println(list[i].toString());
		}

		System.out.println("-------------------------SALGO DE LISTSTATUS------------------");

		return list;
	}

	@Override
    	public boolean delete(Path path, boolean recursive){

		System.out.println("------------------ENTRO A DELETE------------------");
		path = removeURI(path);
		
		if (!exists(path)) return false;
		if (!isDir(path)) return this.xpn.jni_xpn_unlink(path.toString()) == 0;
		if (!recursive)
			return this.xpn.jni_xpn_rmdir(path.toString()) == 0;

		String [] str = this.xpn.jni_xpn_getDirContent(path.toString());
		String deletePath;
		boolean res;
		for (int i = 0; i < str.length; i++){
			if (str[i].equals(".") || str[i].equals("..")) continue;
			res = delete(new Path(path.toString() + "/" + str[i]), true);
			if (!res) return false;
		}
		System.out.println("------------------SALGO DE DELETE------------------");
		return this.xpn.jni_xpn_rmdir(path.toString()) == 0;
	}

	@Override
    	public boolean rename(Path src, Path dst){
		src = removeURI(src);
		dst = removeURI(dst);
		System.out.println("------------------ENTRO A RENAME------------------");
		int res = xpn.jni_xpn_rename(src.toString(), dst.toString());
		System.out.println("------------------SALGO DE RENAME------------------");
		return res == 0;
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress){
		System.out.println("------------------ENTRO A APPEND------------------");
		System.out.println("EXPAND INICIALIZADO ----------------------- " + this.initialized);
		f = removeURI(f);
		if (!exists(f)) xpn.jni_xpn_creat(f.toString(), flags.S_IRWXU | flags.S_IRWXG | flags.S_IRWXO);
		System.out.println("------------------SALGO DE APPEND------------------");
		return new FSDataOutputStream(new ExpandOutputStream(f.toString(), bufsize, (short) 0,
                                        blksize, true), statistics);
	}

	@Override
	public FSDataOutputStream append(Path f) throws IOException {
		return append(f, 4096, null);
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
		long blockSize, Progressable progress) throws IOException {
		System.out.println("------------------ENTRO A CREATE------------------");
		f = removeURI(f);
		Path parent = f.getParent();
		if (exists(f)) {
			if (overwrite) delete(f, false);
			else return null;
		}else{
			if (!exists(parent)) mkdirs(parent, FsPermission.getFileDefault());
		}
		System.out.println("------------------SALGO DE CREATE------------------");

		return new FSDataOutputStream(new ExpandOutputStream(f.toString(), bufsize, replication, 
					blksize, false), statistics);
	}

	@Override
	public FSDataOutputStream create(Path f) throws IOException {
		return create(f, FsPermission.getFileDefault(), true, 4096, (short) 0, (long) 4096, null);
	}

	@Override
    	public FSDataInputStream open(Path f, int bufferSize){
		System.out.println("------------------ENTRO A OPEN------------------");
		f = removeURI(f);
		System.out.println("------------------SALGO DE OPEN------------------");
		return new FSDataInputStream(new ExpandFSInputStream(f.toString(), bufsize, statistics));
	}

	@Override
    	public FSDataInputStream open(Path f){
		return open(f, bufsize);
	}

	@Override
	public URI getUri() {
		return this.uri;
	}

	private Path makeAbsolute (Path path) {
		System.out.println("------------------ENTRO A MKABSOLUTE------------------");
		path = removeURI(path);
		String fullPath = this.workingDirectory.toString() + path.toString();
		System.out.println("------------------SALGO DE MKABSOLUTE------------------");
		return new Path (fullPath);
	}
	
	public boolean isDir (Path path) {
		path = removeURI(path);
		System.out.println("-------------------ENTRO A ISDIR----------------------");
		Stat stats = this.xpn.jni_xpn_stat(path.toString());
		System.out.println("----------------SALGO DE ISDIR---------------");
		return this.xpn.jni_xpn_isDir(stats.st_mode) != 0;
	}

	public boolean exists (Path path){
		System.out.println("------------------ENTRO A EXISTS------------------");
		this.xpn.jni_xpn_init();
		System.out.println("EXPAND INICIALIZADO EN EXIST ----------------- " + this.initialized);
		path = removeURI(path);
		Stat stats = this.xpn.jni_xpn_stat(path.toString());
		System.out.println(path.toString());
		if (this.xpn.jni_xpn_stat(path.toString()) == null) return false;
		System.out.println("------------------SALGO DE EXISTS------------------");
		return true;
	}

	private Path removeURI (Path path){
		String str [] = path.toString().split(":");
		if (str.length == 1) return path;
		else return new Path (str[1]);
	}
}
