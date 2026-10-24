/** Ponto de extensão para a identidade de um provedor OIDC externo (Keycloak/Zitadel).
 * A autenticação não foi implementada propositalmente: o desafio não atribui pontos a ela. */
export interface ProviderIdentity {
  providerId: string;
  subject: string;
}
export interface ProviderIdentityPort {
  authenticate(
    authorizationHeader: string | undefined,
  ): Promise<ProviderIdentity>;
}
