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
 * Multiple languages will implement the language map.
 * The language map will enable mapping of the word of
 * various languages to the respective schema inverted index
 * table, column family and column qualifier.
 * @author karan
 *
 */
public interface ILanguageMap {

	final char COL_A = 'a';
	final char COL_B = 'b';
	final char COL_C = 'c';
	final char COL_D = 'd';
	final char COL_E = 'e';
	final char COL_F = 'f';
	final char COL_G = 'g';
	final char COL_H = 'h';
	final char COL_I = 'i';
	final char COL_J = 'j';
	final char COL_K = 'k';
	final char COL_L = 'l';
	final char COL_M = 'm';
	final char COL_N = 'n';
	final char COL_O = 'o';
	final char COL_P = 'p';
	final char COL_Q = 'q';
	final char COL_R = 'r';
	final char COL_S = 's';
	final char COL_T = 't';
	final char COL_Z = 'z';

	final char COL_0 = '0';
	final char COL_1 = '1';
	final char COL_2 = '2';
	final char COL_3 = '3';
	final char COL_4 = '4';
	final char COL_5 = '5';
	final char COL_6 = '6';
	final char COL_7 = '7';
	final char COL_8 = '8';
	final char COL_9 = '9';
	
	final char COL_OTHERS = '_';
	
	final char[] ALL_COLS = new char[] {
			COL_A,COL_B,COL_C,COL_D,COL_E,COL_F,
			COL_G,COL_H,COL_I,COL_J,COL_K,COL_L,
			COL_M,COL_N,COL_O,COL_P,COL_Q,COL_R,
			COL_S,COL_T,COL_Z,COL_0,COL_1,COL_2,
			COL_3,COL_4,COL_5,COL_6,COL_7,COL_8,
			COL_9,COL_OTHERS
	};
	
	final char FAM_3 = '3';
	final char FAM_4 = '4';
	final char FAM_5 = '5';
	final char FAM_6 = '6';
	final char FAM_7 = '7';
	final char FAM_8 = '8';
	final char FAM_9 = '9';
	final char FAM_10 = '0';
	
	final char[] ALL_FAMS = new char[] {
			COL_3,COL_4,COL_5,COL_6,COL_7,COL_8,
			COL_9,FAM_10
	};
	
	final char[] ALL_TABLES = new char[] {
			COL_A,COL_B,COL_C,COL_D,COL_E,COL_F,
			COL_G,COL_H,COL_I,COL_J,COL_K,COL_L,
			COL_M,COL_N,COL_O,COL_P,COL_Q,COL_R,
			COL_S,COL_T,COL_Z,COL_0,COL_1,COL_2,
			COL_3,COL_4,COL_5,COL_6,COL_7,COL_8,
			COL_9,COL_OTHERS
	};
	
	char getTableName (String word);
	char getColumnFamily (String word);
	char getColumn (String word);
}
