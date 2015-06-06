/*
 * kbitarray.h
 *
 *  Created on: 4/6/2015
 *      Author: KarlOS
 */

#ifndef KBITARRAY_H_
#define KBITARRAY_H_

#define kbitarray_any_is_set(self)		(kbitarray_find_first_set(self) != -1)
#define kbitarray_any_is_clean(self)	(kbitarray_find_first_clean(self) != -1)
#define kbitarray_all_are_clean(self)	!kbitarray_any_is_set(self)
#define kbitarray_all_are_set(self)		!kbitarray_any_is_clean(self)

#include <stdbool.h>
#include <limits.h>
#include <unistd.h>

#define CHAR_CLEAN 	0
#define CHAR_SET 	-1

typedef struct {
	char *bitarray;
	size_t char_count;
	size_t size;
} t_kbitarray;

/* position of bit within character */
#define BIT_CHAR(bit)         ((bit) / CHAR_BIT)

/* array index for character containing bit */
#define BIT_IN_CHAR(bit)      (0x80 >> (CHAR_BIT - 1 - ((bit)  % CHAR_BIT)))

/**
 * @NAME: kbitarray_create
 * @DESC: Crea y devuelve un puntero a una estructura t_kbitarray
 * @PARAMS:
 *		cant_bits - Tamaño en bits del kbitarray que quiero crear
 */
t_kbitarray *kbitarray_create(size_t cant_bits);

/**
 * @NAME: kbitarray_create_and_clean_all
 * @DESC: Crea y devuelve un puntero a una estructura t_kbitarray inicializada en 0
 * @PARAMS:
 *		cant_bits - Tamaño en bits del kbitarray que quiero crear
 */
t_kbitarray* kbitarray_create_and_clean_all(size_t cant_bits);

/**
 * @NAME: kbitarray_clean_all
 * @DESC: Limpia el valor de todos los bits
 */
void kbitarray_clean_all(t_kbitarray* self);

/**
 * @NAME: kbitarray_set_all
 * @DESC: Setea el valor de todos los bits
 */
void kbitarray_set_all(t_kbitarray* self);

/**
 * @NAME: bitarray_test_bit
 * @DESC: Devuelve el valor del bit de la posicion indicada
 */
bool kbitarray_test_bit(t_kbitarray*, off_t bit_index);

/**
 * @NAME: bitarray_set_bit
 * @DESC: Setea el valor del bit de la posicion indicada
 */
void kbitarray_set_bit(t_kbitarray*, off_t bit_index);

/**
 * @NAME: bitarray_clean_bit
 * @DESC: Limpia el valor del bit de la posicion indicada
 */
void kbitarray_clean_bit(t_kbitarray*, off_t bit_index);

/**
 * @NAME: kbitarray_amount_bits_set
 * @DESC: Devuelve la cantidad de bits seteados
 */
size_t kbitarray_amount_bits_set(t_kbitarray* self);

/**
 * @NAME: kbitarray_amount_bits_clean
 * @DESC: Devuelve la cantidad de bits limpios
 */
size_t kbitarray_amount_bits_clean(t_kbitarray* self);

/**
 * @NAME: kbitarray_get_size_in_bits
 * @DESC: Devuelve la cantidad de bits del kbitarray
 */
size_t kbitarray_get_size_in_bits(t_kbitarray* self);

/**
 * @NAME: kbitarray_get_size_in_chars
 * @DESC: Devuelve la cantidad de chars del kbitarray
 */
size_t kbitarray_get_size_in_chars(t_kbitarray* self);

/**
 * @NAME: kbitarray_find_first_set
 * @DESC: Devuelve la posición del primer bit seteado
 * en el kbitarray o -1 en caso de no haber
 */
off_t kbitarray_find_first_set(t_kbitarray* self);

/**
 * @NAME: kbitarray_find_first_clean
 * @DESC: Devuelve la posición del primer bit seteado
 * en el kbitarray o -1 en caso de no haber
 */
off_t kbitarray_find_first_clean(t_kbitarray* self);

/**
 * @NAME: bitarray_destroy
 * @DESC: Destruye el bit array
 */
void kbitarray_destroy(t_kbitarray*);

#endif /* KBITARRAY_H_ */
