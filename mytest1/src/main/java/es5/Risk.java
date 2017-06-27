package es5;

import java.util.Date;

/**
 * @author yuan.li
 */
public class Risk {
	// 设置主键_id
	// private Integer id;
	private String name;
	private Integer age;
	private String addr;
	private String job;
	private String phone;
	private String email;
	private String company;
	private Date birth;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
	}

	public Date getBirth() {
		return birth;
	}

	public void setBirth(Date birth) {
		this.birth = birth;
	}

	@Override
	public String toString() {
		return "Risk [name=" + name + ", age=" + age + ", addr=" + addr + ", job=" + job + ", phone=" + phone
				+ ", email=" + email + ", company=" + company + ", birth=" + birth + "]";
	}
}
