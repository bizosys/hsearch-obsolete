/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.schema;

/**
 * It maps HBase Table, Column Family and Column Qualifier 
 * for a given english word.
 * @author karan
 *
 */
public class EnglishMap implements ILanguageMap {

	public char getTableName(String word) {
		char c = word.charAt(0);
		char table = langMap(c);
		return table;
	}

	public char getColumnFamily(String word) {
		int len = word.length();
		char c;
		switch (len) {
			case 0:
			case 1:
			case 2:
			case 3:
				c = FAM_3;
				break;
			case 4:
				c = FAM_4;
				break;
			case 5:
				c = FAM_5;
				break;
			case 6:
				c = FAM_6;
				break;
			case 7:
				c = FAM_7;
				break;
			case 8:
				c = FAM_8;
				break;
			case 9:
				c = FAM_9;
				break;

			default:
				c = FAM_10;
		}
		return c;
	}

	public char getColumn(String word) {
		char c = word.charAt(word.length() - 1);
		return (langMap(c));
	}


	private char langMap(char c) {
		switch (c) {
			case 'A':
				c = COL_A;
				break;
			case 'a':
				c = COL_A;
				break;
			case 'B':
				c = COL_B;
				break;
			case 'b':
				c = COL_B;
				break;
			case 'C':
				c = COL_C;
				break;
			case 'c':
				c = COL_C;
				break;
			case 'D':
				c = COL_D;
				break;
			case 'd':
				c = COL_D;
				break;
			case 'E':
				c = COL_E;
				break;
			case 'e':
				c = COL_E;
				break;
			case 'F':
				c = COL_F;
				break;
			case 'f':
				c = COL_F;
				break;
			case 'G':
				c = COL_G;
				break;
			case 'g':
				c = COL_G;
				break;
			case 'H':
				c = COL_H;
				break;
			case 'h':
				c = COL_H;
				break;
			case 'I':
				c = COL_I;
				break;
			case 'i':
				c = COL_I;
				break;
			case 'J':
				c = COL_J;
				break;
			case 'j':
				c = COL_J;
				break;
			case 'k':
				c = COL_K;
				break;
			case 'K':
				c = COL_K;
				break;
			case 'l':
				c = COL_L;
				break;
			case 'L':
				c = COL_L;
				break;
			case 'm':
				c = COL_M;
				break;
			case 'M':
				c = COL_M;
				break;
			case 'n':
				c = COL_N;
				break;
			case 'N':
				c = COL_N;
				break;
			case 'o':
				c = COL_O;
				break;
			case 'O':
				c = COL_O;
				break;
			case 'p':
				c = COL_P;
				break;
			case 'P':
				c = COL_P;
				break;
			case 'q':
				c = COL_Q;
				break;
			case 'Q':
				c = COL_Q;
				break;
			case 'r':
				c = COL_R;
				break;
			case 'R':
				c = COL_R;
				break;
			case 's':
				c = COL_S;
				break;
			case 'S':
				c = COL_S;
				break;
			case 't':
				c = COL_T;
				break;
			case 'T':
				c = COL_T;
				break;
			case 'u':
				c = COL_Z;
				break;
			case 'U':
				c = COL_Z;
				break;
			case 'v':
				c = COL_Z;
				break;
			case 'V':
				c = COL_Z;
				break;
			case 'w':
				c = COL_Z;
				break;
			case 'W':
				c = COL_Z;
				break;
			case 'x':
				c = COL_Z;
				break;
			case 'X':
				c = COL_Z;
				break;
			case 'y':
				c = COL_Z;
				break;
			case 'Y':
				c = COL_Z;
				break;
			case 'z':
				c = COL_Z;
				break;
			case 'Z':
				c = COL_Z;
				break;
			case '1':
				c = COL_1;
				break;
			case '2':
				c = COL_2;
				break;
			case '3':
				c = COL_3;
				break;
			case '4':
				c = COL_4;
				break;
			case '5':
				c = COL_5;
				break;
			case '6':
				c = COL_6;
				break;
			case '7':
				c = COL_7;
				break;
			case '8':
				c = COL_8;
				break;
			case '9':
				c = COL_9;
				break;

			default:
				c = COL_OTHERS;
		}
		return c;
	}

}
