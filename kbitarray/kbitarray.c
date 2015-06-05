/*
 * kbitarray.c
 *
 *  Created on: 4/6/2015
 *      Author: KarlOS
 */

/*
 * Copyright (C) 2012 Sistemas Operativos - UTN FRBA. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <stdlib.h>
#include "../kbitarray/kbitarray.h"

//---------------------------------------------------------------------------
t_kbitarray *kbitarray_create(size_t cant_bits) {
	t_kbitarray *self = malloc(sizeof(t_kbitarray));
	char* bitarray;

	self->size = cant_bits;
	self->char_count = cant_bits / CHAR_BIT;

	if (cant_bits % CHAR_BIT != 0) {
		(self->char_count)++;
	}

	bitarray = malloc((self->char_count) * sizeof(char));

	self->bitarray = bitarray;

	return self;
}

//---------------------------------------------------------------------------
void kbitarray_clean_all(t_kbitarray* self) {
	int i;

	for (i = 0; i < self->char_count; i++) {
		self->bitarray[i] = CHAR_CLEAN;
	}
}

//---------------------------------------------------------------------------
t_kbitarray* kbitarray_create_and_clean_all(size_t cant_bits) {
	t_kbitarray* self = kbitarray_create(cant_bits);
	kbitarray_clean_all(self);

	return self;
}

//---------------------------------------------------------------------------
void kbitarray_set_all(t_kbitarray* self) {
	int i;

	for (i = 0; i < self->char_count; i++) {
		self->bitarray[i] = CHAR_SET; //XXX TESTME
	}
}

//---------------------------------------------------------------------------
bool kbitarray_test_bit(t_kbitarray *self, off_t bit_index) {
	return ((self->bitarray[BIT_CHAR(bit_index)] & BIT_IN_CHAR(bit_index)) != 0);
}

//---------------------------------------------------------------------------
void kbitarray_set_bit(t_kbitarray *self, off_t bit_index) {
	self->bitarray[BIT_CHAR(bit_index)] |= BIT_IN_CHAR(bit_index);
}

//---------------------------------------------------------------------------
void kbitarray_clean_bit(t_kbitarray *self, off_t bit_index) {
	unsigned char mask;

	/* create a mask to zero out desired bit */
	mask = BIT_IN_CHAR(bit_index);
	mask = ~mask;

	self->bitarray[BIT_CHAR(bit_index)] &= mask;
}

//---------------------------------------------------------------------------
size_t kbitarray_amount_bits_set(t_kbitarray* self) {
	size_t amount = 0;
	off_t bit_index;

	for (bit_index = 0; bit_index < self->size; bit_index++) {
		if (kbitarray_test_bit(self, bit_index)) {
			amount++;
		}
	}

	return amount;
}

//---------------------------------------------------------------------------
size_t kbitarray_amount_bits_clean(t_kbitarray* self) {
	return self->size - kbitarray_amount_bits_set(self);
}

//---------------------------------------------------------------------------
size_t kbitarray_get_size_in_bits(t_kbitarray* self) {
	return self->size;
}

//---------------------------------------------------------------------------
size_t kbitarray_get_size_in_chars(t_kbitarray* self) {
	return self->char_count;
}

//---------------------------------------------------------------------------
off_t kbitarray_find_first_char_not_clean(t_kbitarray* self) {

	off_t char_index;

	for (char_index = 0; (char_index < self->char_count) && (self->bitarray[char_index] == CHAR_CLEAN); char_index++)
		;

	return (char_index < self->char_count) ? char_index : -1;
}

//---------------------------------------------------------------------------
off_t kbitarray_find_first_char_not_set(t_kbitarray* self) {

	off_t char_index;

	for (char_index = 0; (char_index < self->char_count) && (self->bitarray[char_index] == CHAR_SET); char_index++)
		;

	return (char_index < self->char_count) ? char_index : -1;
}

//---------------------------------------------------------------------------
off_t kbitarray_find_first_set(t_kbitarray* self) {

	off_t char_index;

	if ((char_index = kbitarray_find_first_char_not_clean(self)) != -1)
		return -1;

	char_index *= CHAR_BIT;

	off_t bit_index;
	for (bit_index = char_index;
			(bit_index < char_index + CHAR_BIT) && (bit_index < self->size) && (!kbitarray_test_bit(self, bit_index));
			bit_index++)
		;

	return ((bit_index < char_index + CHAR_BIT) && (bit_index < self->size)) ? bit_index : -1;
}

//---------------------------------------------------------------------------
off_t kbitarray_find_first_clean(t_kbitarray* self) {

	off_t char_index;

	if ((char_index = kbitarray_find_first_char_not_set(self)) != -1)
		return -1;

	char_index *= CHAR_BIT;

	off_t bit_index;
	for (bit_index = char_index;
			(bit_index < char_index + CHAR_BIT) && (bit_index < self->size) && (kbitarray_test_bit(self, bit_index)); bit_index++)
		;

	return ((bit_index < char_index + CHAR_BIT) && (bit_index < self->size)) ? bit_index : -1;
}

//---------------------------------------------------------------------------
void kbitarray_destroy(t_kbitarray* self) {
	free(self->bitarray);
	free(self);
}
