package com.oracle.project.user.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserDimention implements WritableComparable<UserDimention> {
	private String date;
	private String en;
	private String u_uid;
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getEn() {
		return en;
	}

	public void setEn(String en) {
		this.en = en;
	}

	public String getU_uid() {
		return u_uid;
	}

	public void setU_uid(String u_uid) {
		this.u_uid = u_uid;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(en);
		out.writeUTF(u_uid);
	}

	public void readFields(DataInput in) throws IOException {
		this.date = in.readUTF();
		this.en = in.readUTF();
		this.u_uid = in.readUTF();
	}

	public int compareTo(UserDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = this.date.compareTo(o.date);
		
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.en.compareTo(o.en);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.u_uid.compareTo(o.u_uid);
		if(tmp != 0){
			return tmp;
		}

		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		
		result = (result*prime) + this.date == null ? 0 : this.date.hashCode();
		result = (result*prime) + this.en == null ? 0 : this.en.hashCode();
		result = (result*prime) + this.u_uid == null ? 0 : this.u_uid.hashCode();
		
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj){
			return true;
		}
		
		if(this.getClass() != obj.getClass()){
			return false;
		}
		
		UserDimention userDimention = (UserDimention) obj;
		
		if(this.date == null){
			if(userDimention.date != null){
				return false;
			}
		}else if(userDimention.date == null) {
			if(this.date != null){
				return false;
			}
		}else if(!this.date.equals(userDimention.date)) {
			return false;
		}
		
		if(this.en == null){
			if(userDimention.en != null){
				return false;
			}
		}else if(userDimention.en == null) {
			if(this.en != null){
				return false;
			}
		}else if(!this.en.equals(userDimention.en)) {
			return false;
		}
		
		if(this.u_uid == null){
			if(userDimention.u_uid != null){
				return false;
			}
		}else if(userDimention.u_uid == null) {
			if(this.u_uid != null){
				return false;
			}
		}else if(!this.u_uid.equals(userDimention.u_uid)) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.date + "\t" + this.en + "\t" + this.u_uid;
	}
	
}
