package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;



public class SimpleDynamoProvider extends ContentProvider {
	
	public static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider/"+MyDbHelper.TABLE_NAME);
    public SQLiteDatabase db;
	static MyDbHelper myDb;
	int port_avd;
	String node_id,pos;
	int suc_pointer1,suc_pointer2;
	String suc_hash1,suc_hash2,self_hash;
	String self_avd,avd1,avd2;
	String hash0,hash1,hash2;
	int coordinator,rep1,rep2;
	boolean wait=true;
	String Key_found;
	String Value_found,key_recov;
	boolean c,r1,r2;
	int quorum=0,version_tracker=0;
	int flag_alive=0;	
	
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public boolean isalive(int dest) {
		try {
			String msg="P"+" "+(port_avd*2);
			flag_alive=0;
			Thread cli = new Thread(new Client(msg,dest));
			cli.start();	
			try {
			    Thread.sleep(100);
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}
			if(flag_alive==1) {
				return true;
			}			
		} catch(Exception e) {
			e.printStackTrace();
		}
		return false;
	}		

	public String findkeypos(String keyhash) {
		String msg=null;
		if(keyhash.compareTo(hash0)<=0) {
			msg="11112 11108 11116";
			return msg;
		}
		else if(keyhash.compareTo(hash1)<=0) {
			msg="11108 11116 11112";
			return msg;
		}
		else if(keyhash.compareTo(hash2)<=0) {
			msg="11116 11112 11108";
			return msg;
		}
		else if(keyhash.compareTo(hash2)>0) {
			msg="11112 11108 11116";
			return msg;
		}
		return msg;
	}
	
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		db= myDb.getWritableDatabase();    	
    	String key = (String) values.get(MyDbHelper.KEY);
    	String value = (String) values.get(MyDbHelper.VALUE);
    	String version="0";
    	key_recov=key;
    	String keyhash = null;    	
    	try {
			 keyhash = genHash(key);
		} catch (NoSuchAlgorithmException e) {			
			e.printStackTrace();
		}    	
    	
    	Cursor c1= db.rawQuery("select * from "+MyDbHelper.TABLE_NAME+" where key like '"+key+"'", null);
    	if(c1!=null && c1.getCount()>0) {
    		Log.d("values in content provider checking version","Insert");
    		int ind=c1.getColumnIndex(MyDbHelper.VERSION);
			while(c1.moveToNext()) {
				version_tracker=Integer.parseInt(c1.getString(ind))+1;
				version=Integer.toString(version_tracker);
				break;
				
			}	    					
    	}
    	else {
    		Log.d("First time version","Insert");
    		version_tracker=1;
    		version=Integer.toString(version_tracker);
    	}
    		
    	if(keyhash.compareTo(self_hash)<=0) {
    		r1=isalive(suc_pointer1);
    		r2=isalive(suc_pointer2);
    		if(r1==true) {
    			String msg1="I"+" "+key+" "+value+" "+version;
    			Thread cli = new Thread(new Client(msg1,suc_pointer1));
    			cli.start();
    		}
    		if(r2==true) {
    			String msg1="I"+" "+key+" "+value+" "+version;
    			Thread cli = new Thread(new Client(msg1,suc_pointer2));
    			cli.start();
    		}    
    		Log.d("Content Provider","Insert");
    		Cursor c=db.rawQuery("select * from "+MyDbHelper.TABLE_NAME+" where key like '"+key+"'", null);
    		if(c!=null && c.getCount()>0) {
    				Log.d("values in content provider","Insert");
    				int a = db.delete(MyDbHelper.TABLE_NAME,MyDbHelper.KEY+"="+key, null);
    		}   
    		values.put(MyDbHelper.VERSION,version);
			long rowId = db.insert(MyDbHelper.TABLE_NAME, MyDbHelper.VALUE, values);						
			if(rowId > 0){
				Uri newuri = ContentUris.withAppendedId(CONTENT_URI, rowId);
				getContext().getContentResolver().notifyChange(uri, null);
				return newuri;
			}    		
    	}    	
    	else {
    		String nodes=findkeypos(keyhash);
    		StringTokenizer s=new StringTokenizer(nodes," ");
    		coordinator=Integer.parseInt(s.nextToken());
    		rep1=Integer.parseInt(s.nextToken());
    		rep2=Integer.parseInt(s.nextToken());
    		if(isalive(coordinator)==false) {
    			quorum=2;
    			String msg1="I"+" "+key+" "+value+" "+version;
    			Thread cli = new Thread(new Client(msg1,rep1));
    			cli.start();
    			Thread cli1 = new Thread(new Client(msg1,rep2));
    			cli1.start();
    		}
    		else if((isalive(coordinator)==true) && (isalive(rep1)==true) && (isalive(rep2)==true)) {
    			quorum=3;
    			String msg1="I"+" "+key+" "+value+" "+version;
    			Thread cli = new Thread(new Client(msg1,coordinator));
    			cli.start();
    			Thread cli1 = new Thread(new Client(msg1,rep1));
    			cli1.start();
    			Thread cli2 = new Thread(new Client(msg1,rep2));
    			cli2.start();
    		}
    		else {
    			if(isalive(rep1)==false) {
        			String msg1="I"+" "+key+" "+value+" "+version;
        			Thread cli = new Thread(new Client(msg1,coordinator));
        			cli.start();
        			Thread cli1 = new Thread(new Client(msg1,rep2));
        			cli1.start();
    			}
    			else if(isalive(rep2)==false) {
        			String msg1="I"+" "+key+" "+value+" "+version;
        			Thread cli = new Thread(new Client(msg1,coordinator));
        			cli.start();
        			Thread cli1 = new Thread(new Client(msg1,rep1));
        			cli1.start();
    			}
    		}
    	}    	   	
		return null;
	}

	@Override
	public boolean onCreate() {
		myDb = new MyDbHelper(getContext());
    	db = myDb.getWritableDatabase();
    	
    	TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        self_avd = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        port_avd=Integer.parseInt(self_avd);
        try {
        	self_hash=genHash(self_avd);
			hash0=genHash("5556");
			hash1=genHash("5554");
			hash2=genHash("5558");
		} catch (NoSuchAlgorithmException e) {			
			e.printStackTrace();
		}        
        
        if(self_avd.compareTo("5556")==0) {
        	suc_pointer1=11108;
        	suc_pointer2=11116;
        }        
        else if(self_avd.compareTo("5554")==0) {
        	suc_pointer1=11116;
        	suc_pointer2=11112;
        }
        else if(self_avd.compareTo("5558")==0) {
        	suc_pointer1=11112;
        	suc_pointer2=11108;
        }

        Thread serv = new Thread(new Server());
        serv.start();
        
        Thread recov=new Thread(new Recovery());
        recov.start();
                    	    
		return false;
	}
	
	public class Recovery implements Runnable {
		public void run() {
			try {
				String msg,key,pos;
				int coordinator=0,ret_addr=(port_avd*2);				
				for(int i=0;i<20;i++) {
					key=Integer.toString(i);
					pos=findkeypos(key);
					StringTokenizer s=new StringTokenizer(pos," ");
					coordinator=Integer.parseInt(s.nextToken());
					msg="R"+" "+key+" "+ret_addr;
					if(coordinator==(port_avd*2)) {
						Thread cli1 = new Thread(new Client(msg,suc_pointer1));
	        			cli1.start();
					}
					else {
						Thread cli1 = new Thread(new Client(msg,coordinator));
	        			cli1.start();
					}
				}																	
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public class Server implements Runnable {
    	private String msg1=null;
    	public void run() {
    		try {
    			ServerSocket serv=new ServerSocket(10000);
    			while(true) {
    				Socket cl = serv.accept();
    				try {
    					BufferedReader in = new BufferedReader(new InputStreamReader(cl.getInputStream()));
    					msg1=null;
    					msg1 = in.readLine();
    					Thread par=new Thread(new Parser(msg1));
    					par.start();
    					in.close();
    				} catch(Exception e) {
    					e.printStackTrace();
    				}
    				cl.close();
    			}
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    }

	public class Client implements Runnable {
    	String msg;
    	int send_port;
    	
    	public Client(String m,int send) {
    		msg=m;
    		send_port=send;
    	}    	
    	public void run() {
    		try {
    			Socket socket=new Socket("10.0.2.2",send_port);  
    			socket.setSoTimeout(100);
    				try {
    					BufferedWriter out = new BufferedWriter(new OutputStreamWriter (socket.getOutputStream()));
    					out.write(msg);
    					out.flush();
        				} catch (Exception e) {
        					e.printStackTrace();
        				}    				         		
    			socket.close();    			
            } catch (SocketTimeoutException e){
            	flag_alive=0;
            	
            } catch (UnknownHostException e) {
   				e.printStackTrace();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}    	
    }
	
	public class Parser implements Runnable {
		String msg,cmd;
		public Parser(String m) {
    		msg=m;
    	}		
		public void run() {
			StringTokenizer s=new StringTokenizer(msg," ");
			cmd=s.nextToken();
			char token=msg.charAt(0);
			
			if(token=='I') {
				db= myDb.getWritableDatabase(); 
				String key=s.nextToken();
    			String value=s.nextToken();
    			String version=s.nextToken();    			
    			ContentValues cv = new ContentValues();
    			cv.put(MyDbHelper.KEY,key);
    			cv.put(MyDbHelper.VALUE,value);
    			cv.put(MyDbHelper.VERSION,version);     			
    			Cursor c=db.rawQuery("select * from "+MyDbHelper.TABLE_NAME+" where key like '"+key+"'", null);
    			if(c!=null && c.getCount()>0) {
    				Log.d("values in content provider","Insert");
    				int a = db.delete(MyDbHelper.TABLE_NAME,MyDbHelper.KEY+"="+key, null);
    			}    			    			
    			long rowId = db.insert(MyDbHelper.TABLE_NAME, MyDbHelper.VALUE, cv);
			}
			else if(token=='R') {
				String key=s.nextToken(),msg,msg1;
				int ret_addr=Integer.parseInt(s.nextToken());
				String ret_key,ret_val,ret_ver;
				Cursor c=db.rawQuery("select * from "+MyDbHelper.TABLE_NAME+" where key like '"+key+"'", null);
				c.moveToFirst();
				if(c!=null && c.getCount()>0) {					
					int index_value = c.getColumnIndex(MyDbHelper.VALUE);
					msg1=Integer.toString(index_value);
					Log.d("Inside recovery, Index value:", msg1);
	        		int index_key = c.getColumnIndex(MyDbHelper.KEY);
	        		int index_version = c.getColumnIndex(MyDbHelper.VERSION);
	        		ret_key=c.getString(index_key);
	        		ret_val=c.getString(index_value);
	        		ret_ver=c.getString(index_version);
	        		msg="S"+" "+ret_key+" "+" "+ret_val+" "+ret_ver;
	        		Thread cli1 = new Thread(new Client(msg,ret_addr));
        			cli1.start();
				}
			}
			
			else if(token=='S') {
				String key=s.nextToken();
				String val=s.nextToken();
				String ver=s.nextToken();
				ContentValues cv=new ContentValues();
				cv.put(MyDbHelper.KEY,key);
				cv.put(MyDbHelper.VALUE,val);
				cv.put(MyDbHelper.VERSION,ver);
				Cursor c=db.rawQuery("select * from "+MyDbHelper.TABLE_NAME+" where key like '"+key+"'", null);
	    		if(c!=null && c.getCount()>0) {
	    				Log.d("values in content provider","Insert");
	    				int a = db.delete(MyDbHelper.TABLE_NAME,MyDbHelper.KEY+"="+key, null);
	    		}
	    		long rowId = db.insert(MyDbHelper.TABLE_NAME, MyDbHelper.VALUE, cv);
			}
				
			else if(token=='P') {
				String ret_addr=s.nextToken();
				String msg="A"+" "+"alive";
				Thread cli = new Thread(new Client(msg,Integer.parseInt(ret_addr)));
    			cli.start();
			}	
			else if(token=='A') {
				flag_alive=1;
			}
			else if(token=='Q') {
				String key=s.nextToken();
				String ret_addr=s.nextToken();
				Cursor c= db.rawQuery("select * from "+MyDbHelper.TABLE_NAME+" where key like '"+key+"'", null);
				int ind=c.getColumnIndex(MyDbHelper.VALUE);
				if(c!=null) {
					while(c.moveToNext()) {
						String val=c.getString(ind);
						String msg="T"+" "+key+" "+val;
						Thread cli = new Thread(new Client(msg,Integer.parseInt(ret_addr)));
	    				cli.start();  
					}
					
				}
				c.close();
			}
			else if(token=='T') {
    			Key_found=s.nextToken();
    			Value_found=s.nextToken();
    			wait=false;
    		}
		}
	}
	
	
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,String[] selectionArgs, String sortOrder) {
		Cursor c=null;
		String keyhash=null,msg=null,coor=null,suc=null;
		if(selection==null) {
			c= db.rawQuery("select * from "+MyDbHelper.TABLE_NAME, null);
			return c;
		}
		try {
			 keyhash = genHash(selection);
		} catch (NoSuchAlgorithmException e) {			
			e.printStackTrace();
		}    
		msg=findkeypos(keyhash);
		StringTokenizer s=new StringTokenizer(msg," ");
		coor=s.nextToken();
		suc=s.nextToken();
		if(isalive(Integer.parseInt(coor))) {
			String msg1="Q"+" "+selection+" "+(port_avd*2);
			Thread cli = new Thread(new Client(msg1,Integer.parseInt(coor)));
			cli.start();
			while(wait==true) {
				/* wait for the query to return*/
			}	
			wait=true;
			MatrixCursor m = new MatrixCursor(new String[] {MyDbHelper.KEY,MyDbHelper.VALUE});
			m.newRow().add(Key_found).add(Value_found);				
			return m;
		}
		else if(suc_pointer1==Integer.parseInt(coor)) {
			String msg1="Q"+" "+selection+" "+(port_avd*2);
			Thread cli = new Thread(new Client(msg1,suc_pointer2));
			cli.start();
			while(wait==true) {
				/* wait for the query to return*/
			}	
			wait=true;
			MatrixCursor m = new MatrixCursor(new String[] {MyDbHelper.KEY,MyDbHelper.VALUE});
			m.newRow().add(Key_found).add(Value_found);				
			return m;
		}		
		else {
			String msg1="Q"+" "+selection+" "+(port_avd*2);
			Thread cli = new Thread(new Client(msg1,suc_pointer1));
			cli.start();
			while(wait==true) {
				/* wait for the query to return*/
			}	
			wait=true;
			MatrixCursor m = new MatrixCursor(new String[] {MyDbHelper.KEY,MyDbHelper.VALUE});
			m.newRow().add(Key_found).add(Value_found);				
			return m;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
