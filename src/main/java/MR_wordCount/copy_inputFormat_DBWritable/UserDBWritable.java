package MR_wordCount.copy_inputFormat_DBWritable;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class UserDBWritable implements DBWritable, Writable {
	private int id ;
	private String name ;
	private int age ;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	//hadoop串行化
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
	}

	//hadoop反序列化
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.name = in.readUTF();
		this.age = in.readInt();
	}

	public void write(PreparedStatement ppst) throws SQLException {
		//TODO xxxx
	}

	public void readFields(ResultSet rs) throws SQLException {
		this.id = rs.getInt("id");
		this.name = rs.getString("name");
		this.age = rs.getInt("age");
	}
}
