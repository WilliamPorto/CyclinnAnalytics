/**
 * Wrapper de fetch que injeta o header `X-Usuario` em todas as requisições.
 * Por enquanto o usuário é "admin" (hardcoded); quando tivermos login,
 * bastará ler o valor de algum store/localStorage.
 */

const USUARIO = "admin";

export async function apiFetch(
  url: string,
  opts: RequestInit = {}
): Promise<Response> {
  const headers = new Headers(opts.headers);
  headers.set("X-Usuario", USUARIO);
  return fetch(url, { ...opts, headers });
}
