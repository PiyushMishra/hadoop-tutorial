package com.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.sort.SecondarySort.IntPair;

public class SecondarySortString {

	public static class CompositeKey implements
			WritableComparable<CompositeKey> {

		String firstname = "";
		String lastname = "";

		public void set(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}

		public String getFirstName() {
			return this.firstname;
		}

		public String getLastName() {
			return this.firstname;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			firstname = in.readLine();
			lastname = in.readLine();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeBytes(firstname);
			out.writeBytes(lastname);
		}

		@Override
		public int compareTo(CompositeKey key) {
			// TODO Auto-generated method stub
			if (this.firstname != key.firstname) {
				return -1;
			} else if (this.lastname != key.lastname) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public boolean equals(Object key) {
			CompositeKey compositeKey = (CompositeKey) key;
			return (this.firstname == compositeKey.firstname && this.lastname == compositeKey.lastname);
		}
	}

	
	/** A Comparator that compares serialized CompositeKey. */ 
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(CompositeKey.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
        return compareBytes(b1, s1, l1, b2, s2, l2);
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(IntPair.class, new Comparator());
    }
	
}
